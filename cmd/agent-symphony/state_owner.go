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
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/SysSU/agent-symphony/internal/config"
	internalgithub "github.com/SysSU/agent-symphony/internal/github"
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
	WorkerProfileDigest       string                               `json:"-"`
	Version                   int                                  `json:"version"`
	Repository                string                               `json:"repository"`
	Epoch                     uint64                               `json:"epoch"`
	Revision                  uint64                               `json:"revision"`
	IssueGenerations          map[string]uint64                    `json:"issue_generations"`
	AttemptGenerations        map[string]uint64                    `json:"attempt_generations"`
	Attempts                  map[string]runtimeAttemptRecord      `json:"attempts"`
	Observations              map[string]reconciliationObservation `json:"observations"`
	Recoveries                map[string]runtimePRRecovery         `json:"recoveries"`
	Tombstones                map[string]runtimeTombstone          `json:"tombstones"`
	Effects                   map[string]runtimeEffectIntent       `json:"effects"`
	ReviewerProofs            map[string]reviewerProcessProof      `json:"reviewer_proofs"`
	ReviewerSafetyMigrated    bool                                 `json:"reviewer_safety_migrated,omitempty"`
	LegacyReviewerQuarantines map[string]string                    `json:"legacy_reviewer_quarantines"`
	ReviewerRevocationTracked bool                                 `json:"reviewer_revocation_tracked,omitempty"`
	ExternalDispatchTracked   bool                                 `json:"external_dispatch_tracked,omitempty"`
	ControlReceipts           []controlReceipt                     `json:"control_receipts"`
	ControlGenerations        map[string]uint64                    `json:"control_generations,omitempty"`
	ControlRepairs            map[string]controlSnapshotRepair     `json:"control_repairs,omitempty"`
	MachineStatuses           map[string]machineStatusRecord       `json:"machine_statuses,omitempty"`
	CycleDiagnostic           string                               `json:"cycle_diagnostic,omitempty"`
	CycleDiagnosticAt         time.Time                            `json:"cycle_diagnostic_at,omitzero"`
	CycleOutcomeEpoch         uint64                               `json:"cycle_outcome_epoch,omitempty"`
	CycleOutcomeID            uint64                               `json:"cycle_outcome_id,omitempty"`
	CycleOutcomeSource        uint64                               `json:"cycle_outcome_source_revision,omitempty"`
	StaleReconciliations      uint64                               `json:"stale_reconciliations,omitempty"`
}

// machineStatusRecord is the single owner-issued ordering domain for every
// Agent Symphony status producer. Sequence is the durable per-issue order used
// both for optimistic admission and at the GitHub boundary. SourceID makes a
// producer proposal idempotent; SourceSequence is allocated by the owner.
type machineStatusRecord struct {
	Repository        string `json:"repository"`
	Issue             int    `json:"issue"`
	Attempt           int    `json:"attempt"`
	IssueGeneration   uint64 `json:"issue_generation"`
	AttemptGeneration uint64 `json:"attempt_generation,omitempty"`
	Sequence          uint64 `json:"sequence"`
	AppliedSequence   uint64 `json:"applied_sequence,omitempty"`
	Source            string `json:"source"`
	SourceID          string `json:"source_id"`
	SourceSequence    uint64 `json:"source_sequence"`
	Status            string `json:"status"`
	Reason            string `json:"reason"`
}

type controlSnapshotRepair struct {
	Generation uint64 `json:"generation"`
	Body       string `json:"body"`
}

type runtimeAttemptRecord struct {
	Generation       uint64                `json:"generation"`
	ObservationEpoch uint64                `json:"observation_epoch,omitempty"`
	LastCycleID      uint64                `json:"last_cycle_id,omitempty"`
	StopEffectID     string                `json:"stop_effect_id,omitempty"`
	Manifest         agentruntime.Manifest `json:"manifest"`
	WorkerSeal       *workerSealSelection  `json:"worker_seal,omitempty"`
}

type workerSealSelection struct {
	Generation    uint64       `json:"generation"`
	HeadSHA       string       `json:"head_sha"`
	Root          string       `json:"root"`
	BundleSHA256  string       `json:"bundle_sha256"`
	ProfileDigest string       `json:"profile_digest"`
	Result        workerResult `json:"result"`
}

// A completed review's filesystem root outlives bounded operator receipts.
// This owner-held certificate is retained until destructive cleanup commits.
type reviewerProcessProof struct {
	Repository        string `json:"repository"`
	Issue             int    `json:"issue"`
	Attempt           int    `json:"attempt"`
	Target            string `json:"target"`
	Mode              string `json:"mode"`
	EffectID          string `json:"effect_id"`
	IssueGeneration   uint64 `json:"issue_generation"`
	AttemptGeneration uint64 `json:"attempt_generation"`
	GroupPID          int    `json:"group_pid"`
	DeadProved        bool   `json:"dead_proved,omitempty"`
	NeverRan          bool   `json:"never_ran,omitempty"`
	LegacyUnverified  bool   `json:"legacy_unverified,omitempty"`
}

func reviewerProofKey(repository string, issue, attempt int, mode, target string) string {
	return ownerAttemptKey(repository, issue, attempt) + "\x00" + mode + "\x00" + target
}

type runtimePRRecovery struct {
	IssueGeneration   uint64                 `json:"issue_generation"`
	AttemptGeneration uint64                 `json:"attempt_generation"`
	State             internalgithub.PRState `json:"state"`
}

type runtimeTombstone struct {
	Repository            string                                `json:"repository"`
	Issue                 int                                   `json:"issue"`
	Attempt               int                                   `json:"attempt"`
	Action                string                                `json:"action"`
	InvalidatedGeneration uint64                                `json:"invalidated_generation"`
	Generation            uint64                                `json:"generation"`
	Revision              uint64                                `json:"revision"`
	CleanupPhase          string                                `json:"cleanup_phase"`
	PublishedHead         string                                `json:"published_head,omitempty"`
	Manifest              *agentruntime.Manifest                `json:"manifest,omitempty"`
	CleanupPolicy         *agentruntime.EffectCleanupPolicy     `json:"cleanup_policy,omitempty"`
	EffectID              string                                `json:"effect_id,omitempty"`
	ReviewerLeaseID       string                                `json:"reviewer_lease_id,omitempty"`
	Diagnostic            string                                `json:"diagnostic,omitempty"`
	InvalidatedHandoff    *handoffCandidateInvalidation         `json:"invalidated_handoff,omitempty"`
	HandoffCompensated    bool                                  `json:"handoff_compensated,omitempty"`
	InvalidatedStart      *startCandidateInvalidation           `json:"invalidated_start,omitempty"`
	ExternalOutcomes      map[string]invalidatedExternalOutcome `json:"external_outcomes,omitempty"`
}

type invalidatedExternalOutcome struct {
	Action         reconciliationEffectAction `json:"action"`
	Observed       bool                       `json:"observed"`
	Merged         bool                       `json:"merged,omitempty"`
	Superseded     bool                       `json:"superseded,omitempty"`
	PR             int                        `json:"pr,omitempty"`
	HeadSHA        string                     `json:"head_sha,omitempty"`
	StatusSequence uint64                     `json:"status_sequence,omitempty"`
}

type startGateCandidate struct {
	Nonce  string `json:"nonce"`
	MayRun bool   `json:"may_run,omitempty"`
}

type startCandidateInvalidation struct {
	EffectID   string                `json:"effect_id"`
	Manifest   agentruntime.Manifest `json:"manifest"`
	Candidates []startGateCandidate  `json:"candidates"`
}

// The owner retains this candidate after deleting its pending handoff intent.
// External cleanup may act only on its exact durable pane binding.
type handoffCandidateInvalidation struct {
	EffectID string                `json:"effect_id"`
	Token    string                `json:"token"`
	Key      string                `json:"key"`
	Manifest agentruntime.Manifest `json:"manifest"`
}

func validHandoffCandidateInvalidation(candidate handoffCandidateInvalidation, manifest agentruntime.Manifest) bool {
	decoded, err := hex.DecodeString(candidate.EffectID)
	return err == nil && len(decoded) == 16 && agentruntime.ValidLaunchToken(candidate.Token) && candidate.Token != manifest.LaunchToken && candidate.Key != "" && filepath.Base(candidate.Key) == candidate.Key && !strings.ContainsAny(candidate.Key, "/\\\x00\r\n") && candidate.Manifest.Version == agentruntime.ManifestVersion2 && sameAttemptIdentity(candidate.Manifest, manifest) && candidate.Manifest.LaunchToken == manifest.LaunchToken && candidate.Manifest.LaunchID == manifest.LaunchID
}

func validStartCandidates(candidates []startGateCandidate) bool {
	if len(candidates) == 0 {
		return false
	}
	seen := make(map[string]bool, len(candidates))
	for index, candidate := range candidates {
		if !agentruntime.ValidLaunchToken(candidate.Nonce) || seen[candidate.Nonce] || candidate.MayRun && index != len(candidates)-1 {
			return false
		}
		seen[candidate.Nonce] = true
	}
	return true
}

func validStartCandidateInvalidation(candidate startCandidateInvalidation, manifest agentruntime.Manifest) bool {
	return agentruntime.ValidLaunchToken(candidate.EffectID) && candidate.Manifest.Version == agentruntime.ManifestVersion2 && sameAttemptIdentity(candidate.Manifest, manifest) && candidate.Manifest.LaunchToken == manifest.LaunchToken && candidate.Manifest.LaunchID == manifest.LaunchID && validStartCandidates(candidate.Candidates)
}

type runtimeEffectIntent struct {
	ID                                  string                           `json:"id"`
	Action                              string                           `json:"action"`
	Repository                          string                           `json:"repository"`
	Issue                               int                              `json:"issue"`
	Attempt                             int                              `json:"attempt"`
	IssueGeneration                     uint64                           `json:"issue_generation"`
	AttemptGeneration                   uint64                           `json:"attempt_generation"`
	IntentEpoch                         uint64                           `json:"intent_epoch,omitempty"`
	IntentRevision                      uint64                           `json:"intent_revision"`
	State                               string                           `json:"state"`
	Dispatched                          bool                             `json:"dispatched,omitempty"`
	GovernancePhases                    []internalgithub.GovernancePhase `json:"governance_phases,omitempty"`
	RequestDigest                       string                           `json:"request_digest"`
	MachineStatusSequence               uint64                           `json:"machine_status_sequence,omitempty"`
	CandidateLaunchToken                string                           `json:"candidate_launch_token,omitempty"`
	StartGateNonce                      string                           `json:"start_gate_nonce,omitempty"`
	StartMayRun                         bool                             `json:"start_may_run,omitempty"`
	StartCandidates                     []startGateCandidate             `json:"start_candidates,omitempty"`
	Reason                              string                           `json:"reason,omitempty"`
	Review                              *agentruntime.ReviewTransition   `json:"review,omitempty"`
	Reconciliation                      *reconciliationEffectRequest     `json:"reconciliation,omitempty"`
	ReconciliationResult                *reconciliationEffectResult      `json:"reconciliation_result,omitempty"`
	ReviewerLaunched                    bool                             `json:"reviewer_launched,omitempty"`
	ReviewerGateProtocol                bool                             `json:"reviewer_gate_protocol,omitempty"`
	ReviewerSessionRequested            bool                             `json:"reviewer_session_requested,omitempty"`
	ReviewerGroupPID                    int                              `json:"reviewer_group_pid,omitempty"`
	ReviewerResultDigest                string                           `json:"reviewer_result_digest,omitempty"`
	ReviewerStopped                     bool                             `json:"reviewer_stopped,omitempty"`
	ReviewerRevoked                     bool                             `json:"reviewer_revoked,omitempty"`
	SupersededReviewerID                string                           `json:"superseded_reviewer_id,omitempty"`
	SupersededReviewerGroupPID          int                              `json:"superseded_reviewer_group_pid,omitempty"`
	SupersededReviewerGateProtocol      bool                             `json:"superseded_reviewer_gate_protocol,omitempty"`
	SupersededReviewerSessionRequested  bool                             `json:"superseded_reviewer_session_requested,omitempty"`
	SupersededReviewerRequestDigest     string                           `json:"superseded_reviewer_request_digest,omitempty"`
	SupersededReviewerTarget            string                           `json:"superseded_reviewer_target,omitempty"`
	SupersededReviewerMode              string                           `json:"superseded_reviewer_mode,omitempty"`
	SupersededReviewerIssueGeneration   uint64                           `json:"superseded_reviewer_issue_generation,omitempty"`
	SupersededReviewerAttemptGeneration uint64                           `json:"superseded_reviewer_attempt_generation,omitempty"`
	Diagnostic                          string                           `json:"diagnostic,omitempty"`
	InvalidatedHandoff                  *handoffCandidateInvalidation    `json:"invalidated_handoff,omitempty"`
	InvalidatedStart                    *startCandidateInvalidation      `json:"invalidated_start,omitempty"`
}

type stateResultIdentity struct {
	Epoch             uint64
	SourceRevision    uint64
	CycleID           uint64
	IssueGeneration   uint64
	AttemptGeneration uint64
	EffectID          string
	RequestDigest     string
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
	CleanupPolicy             *agentruntime.EffectCleanupPolicy
	Diagnostic                string
	EffectAction              string
	EffectRequestDigest       string
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

type beginRuntimeEffectCommand struct {
	Identity             stateResultIdentity
	Action               agentruntime.EffectAction
	Manifest             agentruntime.Manifest
	Reason               string
	RequestDigest        string
	Review               *agentruntime.ReviewTransition
	SupersededReviewerID string
	CandidateLaunchToken string
	StartGateNonce       string
}

type finishRuntimeEffectCommand struct {
	Identity stateResultIdentity
	Action   agentruntime.EffectAction
	Manifest agentruntime.Manifest
}

type authorizeRuntimeEffectCommand struct {
	Identity  stateResultIdentity
	Action    agentruntime.EffectAction
	GateNonce string
}

type rotateStartGateCommand struct {
	Identity stateResultIdentity
	OldNonce string
	NewNonce string
}

type diagnoseRuntimeEffectCommand struct {
	Identity   stateResultIdentity
	Action     agentruntime.EffectAction
	Diagnostic string
}

type beginReconciliationEffectCommand struct {
	Identity stateResultIdentity
	Request  reconciliationEffectRequest
}

type authorizeReconciliationEffectCommand struct {
	Identity stateResultIdentity
	Action   reconciliationEffectAction
}

type selectWorkerSealCommand struct {
	Repository         string
	Issue, Attempt     int
	ExpectedGeneration uint64
	Selection          workerSealSelection
}

type admitMachineStatusCommand struct {
	Repository                    string
	Issue, Attempt                int
	ExpectedIssueGeneration       uint64
	ExpectedAttemptGeneration     uint64
	ExpectedObservationGeneration uint64
	ExpectedStatusSequence        uint64
	Dependency, PullRequest       int
	Source                        string
	SourceID                      string
	Status, Reason                string
}

type resolveInvalidatedReconciliationEffectCommand struct {
	Identity stateResultIdentity
	Outcome  invalidatedExternalOutcome
}

type markPlanReviewRunningCommand struct {
	Identity stateResultIdentity
	GroupPID int
}

type markReviewerSessionRequestedCommand struct {
	Identity stateResultIdentity
}

type proveReviewerDeadCommand struct {
	Identity stateResultIdentity
	GroupPID int
	NeverRan bool
}

type sealReviewerResultCommand struct {
	Identity stateResultIdentity
	Result   reconciliationEffectResult
	Pane     reviewerPaneIdentity
	Terminal reviewerTerminalRecord
}

type markReviewerStoppedCommand struct {
	Identity    stateResultIdentity
	Observation reviewerStopObservation
}

type bindReviewerStoppingCommand struct {
	Identity stateResultIdentity
	GroupPID int
}

type reviewerStopObservation struct {
	GroupPID int
	NeverRan bool
}

type supersedePlanReviewCommand struct {
	Identity       stateResultIdentity
	CurrentHeadSHA string
}

type finishReconciliationEffectCommand struct {
	Identity stateResultIdentity
	Result   reconciliationEffectResult
}

type diagnoseReconciliationEffectCommand struct {
	Identity   stateResultIdentity
	Action     reconciliationEffectAction
	Diagnostic string
}

type mutatePRRecoveryCommand struct {
	Identity stateResultIdentity
	Kind     prRecoveryMutationKind
	State    internalgithub.PRState
	Feedback internalgithub.Feedback
}

type mutateGovernancePhaseCommand struct {
	Identity stateResultIdentity
	Phase    internalgithub.GovernancePhase
	Complete bool
}

type recordControlReceiptCommand struct {
	Receipt controlReceipt
}

type recordOperatorDiagnosticCommand struct {
	RequestID  string
	Phase      string
	EffectID   string
	Diagnostic string
}

type completeHandoffCompensationCommand struct {
	RequestID string
	Proof     handoffCompensationProof
}

type beginOperatorMutationCommand struct {
	Request               controlRequest
	Identity              stateResultIdentity
	ObservationGeneration uint64
	ObservationCycleID    uint64
	ObservationBodyDigest string
	Manifest              agentruntime.Manifest
	PublishedHead         string
	CleanupDigest         string
	CleanupPolicy         agentruntime.EffectCleanupPolicy
	IssueClosed           bool
	RemoteOnly            bool
	CleanupValid          bool
	LivenessFailed        bool
	Runtime               *beginRuntimeEffectCommand
	Reconciliation        *beginReconciliationEffectCommand
}

type startOperatorCleanupCommand struct {
	Identity stateResultIdentity
}

type finishOperatorRuntimeEffectCommand struct {
	Finish finishRuntimeEffectCommand
}

type finishOperatorReconciliationEffectCommand struct {
	Finish finishReconciliationEffectCommand
}

type advanceOperatorRecoveryCommand struct {
	RequestID      string
	Identity       stateResultIdentity
	Reconciliation beginReconciliationEffectCommand
}

type stateOwnerCommandKind uint8

const (
	stateOwnerStart stateOwnerCommandKind = iota + 1
	stateOwnerAdvanceIssueGeneration
	stateOwnerInvalidateAttempt
	stateOwnerRecordEffect
	stateOwnerCompleteEffect
	stateOwnerBeginRuntimeEffect
	stateOwnerFinishRuntimeEffect
	stateOwnerAuthorizeRuntimeEffect
	stateOwnerRotateStartGate
	stateOwnerDiagnoseRuntimeEffect
	stateOwnerApplyReconciliation
	stateOwnerBeginReconciliationEffect
	stateOwnerAuthorizeReconciliationEffect
	stateOwnerResolveInvalidatedReconciliationEffect
	stateOwnerSelectWorkerSeal
	stateOwnerAdmitMachineStatus
	stateOwnerMarkPlanReviewRunning
	stateOwnerMarkReviewerSessionRequested
	stateOwnerProveReviewerDead
	stateOwnerSealReviewerResult
	stateOwnerMarkReviewerStopped
	stateOwnerBindReviewerStopping
	stateOwnerSupersedePlanReview
	stateOwnerFinishReconciliationEffect
	stateOwnerDiagnoseReconciliationEffect
	stateOwnerMutatePRRecovery
	stateOwnerMutateGovernancePhase
	stateOwnerRecordControlReceipt
	stateOwnerRecordOperatorDiagnostic
	stateOwnerCompleteHandoffCompensation
	stateOwnerBeginOperatorMutation
	stateOwnerStartOperatorCleanup
	stateOwnerFinishOperatorRuntimeEffect
	stateOwnerFinishOperatorReconciliationEffect
	stateOwnerAdvanceOperatorRecovery
	stateOwnerRecordCycleOutcome
)

type stateOwnerCommand struct {
	kind                         stateOwnerCommandKind
	issue                        advanceIssueGenerationCommand
	invalidate                   invalidateAttemptCommand
	record                       recordEffectCommand
	complete                     completeEffectCommand
	begin                        beginRuntimeEffectCommand
	finish                       finishRuntimeEffectCommand
	authorize                    authorizeRuntimeEffectCommand
	rotateStartGate              rotateStartGateCommand
	diagnoseRuntime              diagnoseRuntimeEffectCommand
	reconcile                    applyReconciliationCommand
	beginReconciliation          beginReconciliationEffectCommand
	authorizeReconciliation      authorizeReconciliationEffectCommand
	resolveInvalidatedReconcile  resolveInvalidatedReconciliationEffectCommand
	selectWorkerSeal             selectWorkerSealCommand
	machineStatus                admitMachineStatusCommand
	markPlanReviewRunning        markPlanReviewRunningCommand
	markReviewerSessionRequested markReviewerSessionRequestedCommand
	proveReviewerDead            proveReviewerDeadCommand
	sealReviewerResult           sealReviewerResultCommand
	markReviewerStopped          markReviewerStoppedCommand
	bindReviewerStopping         bindReviewerStoppingCommand
	supersedePlanReview          supersedePlanReviewCommand
	finishReconciliation         finishReconciliationEffectCommand
	diagnoseReconciliation       diagnoseReconciliationEffectCommand
	mutatePRRecovery             mutatePRRecoveryCommand
	mutateGovernancePhase        mutateGovernancePhaseCommand
	receipt                      recordControlReceiptCommand
	operatorDiagnostic           recordOperatorDiagnosticCommand
	completeHandoff              completeHandoffCompensationCommand
	beginOperator                beginOperatorMutationCommand
	startOperatorCleanup         startOperatorCleanupCommand
	finishOperatorRuntime        finishOperatorRuntimeEffectCommand
	finishOperatorReconcile      finishOperatorReconciliationEffectCommand
	advanceOperatorRecovery      advanceOperatorRecoveryCommand
	cycleOutcome                 recordCycleOutcomeCommand
	context                      context.Context
	arbitration                  *stateOwnerCommandArbitration
	reply                        chan stateOwnerResult
}

type stateOwnerCommandArbitration struct{ state atomic.Uint32 }

const (
	stateOwnerCommandCanceled uint32 = iota + 1
	stateOwnerCommandClaimed
)

type recordCycleOutcomeCommand struct {
	Identity   stateResultIdentity
	Diagnostic string
	At         time.Time
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

type statePersistenceRequest struct {
	state runtimeOwnerState
	reply chan error
}

type statePersistenceInstalledError struct{ err error }

func (e statePersistenceInstalledError) Error() string { return e.err.Error() }
func (e statePersistenceInstalledError) Unwrap() error { return e.err }

type pendingStateCommit struct {
	command   stateOwnerCommand
	candidate runtimeOwnerState
	effect    *runtimeEffectIntent
}

type appliedReconciliationCycle struct {
	cycle  uint64
	digest string
}

// stateOwner is the sole in-process commit authority for the runtime ledger.
type stateOwner struct {
	stateRoot   string
	attemptRoot string
	commands    chan stateOwnerCommand
	snapshots   chan stateOwnerSnapshotRequest
	stop        chan struct{}
	stopOnce    sync.Once
	done        chan struct{}
	commits     chan stateOwnerSnapshot
}

func startStateOwner(ctx context.Context, stateRoot, attemptRoot string, initial runtimeOwnerState, persist func(runtimeOwnerState) error) (*stateOwner, error) {
	if persist == nil {
		return nil, errors.New("state persistence is required")
	}
	root, err := filepath.EvalSymlinks(stateRoot)
	if err != nil || root != filepath.Clean(stateRoot) {
		return nil, errors.New("runtime state root is unsafe")
	}
	attempts, err := filepath.EvalSymlinks(attemptRoot)
	if err != nil || attempts != filepath.Clean(attemptRoot) {
		return nil, errors.New("runtime attempt root is unsafe")
	}
	migrateLegacyMachineStatuses(&initial)
	if err := validateRuntimeOwnerState(initial, attempts, root, false); err != nil {
		return nil, err
	}
	owner := &stateOwner{
		stateRoot:   root,
		attemptRoot: attempts,
		commands:    make(chan stateOwnerCommand),
		snapshots:   make(chan stateOwnerSnapshotRequest),
		stop:        make(chan struct{}),
		done:        make(chan struct{}),
		commits:     make(chan stateOwnerSnapshot, 1),
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
	defer close(o.commits)
	committed := cloneRuntimeOwnerState(initial)
	var queue []stateOwnerCommand
	var inFlight *pendingStateCommit
	var persistenceResult <-chan error
	stopping := false
	var poisoned error
	var cycleID uint64
	appliedCycles := map[string]appliedReconciliationCycle{}
	publish := func() {
		snapshot := stateOwnerSnapshot{State: cloneRuntimeOwnerState(committed)}
		select {
		case o.commits <- snapshot:
		default:
			select {
			case <-o.commits:
			default:
			}
			o.commits <- snapshot
		}
	}

	startNext := func() {
		for inFlight == nil && len(queue) > 0 {
			command := queue[0]
			queue = queue[1:]
			if command.arbitration != nil && command.arbitration.state.Load() == stateOwnerCommandCanceled {
				command.reply <- stateOwnerResult{err: commandCancellationError(command)}
				continue
			}
			if poisoned != nil {
				command.reply <- stateOwnerResult{err: poisoned}
				continue
			}
			candidate, effect, err := applyStateOwnerCommand(o.attemptRoot, o.stateRoot, committed, command, appliedCycles)
			if err != nil {
				command.reply <- stateOwnerResult{err: err}
				continue
			}
			if reflect.DeepEqual(candidate, committed) {
				if !claimStateOwnerCommand(command) {
					command.reply <- stateOwnerResult{err: commandCancellationError(command)}
					continue
				}
				recordAppliedReconciliationCycles(appliedCycles, command, committed, false)
				command.reply <- stateOwnerResult{snapshot: stateOwnerSnapshot{State: cloneRuntimeOwnerState(committed)}, effect: cloneEffect(effect)}
				continue
			}
			reply := make(chan error, 1)
			request := statePersistenceRequest{state: candidate, reply: reply}
			if !claimStateOwnerCommand(command) {
				command.reply <- stateOwnerResult{err: commandCancellationError(command)}
				continue
			}
			persistence <- request
			inFlight = &pendingStateCommit{command: command, candidate: candidate, effect: effect}
			persistenceResult = reply
		}
	}

	finish := func() bool {
		if !stopping || inFlight != nil {
			return false
		}
		close(persistence)
		<-persistenceDone
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
		stopInput := (<-chan struct{})(o.stop)
		if stopping {
			stopInput = nil
		}
		select {
		case command := <-commandInput:
			if stopping {
				command.reply <- stateOwnerResult{err: errStateOwnerStopped}
			} else if poisoned != nil {
				command.reply <- stateOwnerResult{err: poisoned}
			} else {
				queue = append(queue, command)
			}
		case request := <-o.snapshots:
			if stopping {
				request.reply <- stateOwnerResult{err: errStateOwnerStopped}
			} else if poisoned != nil {
				request.reply <- stateOwnerResult{err: poisoned}
				continue
			}
			id := uint64(0)
			if request.cycle {
				cycleID++
				id = cycleID
			}
			request.reply <- stateOwnerResult{snapshot: stateOwnerSnapshot{State: cloneRuntimeOwnerState(committed), CycleID: id}}
		case err := <-persistenceResult:
			if err == nil {
				committed = inFlight.candidate
				publish()
				recordAppliedReconciliationCycles(appliedCycles, inFlight.command, committed, true)
				inFlight.command.reply <- stateOwnerResult{snapshot: stateOwnerSnapshot{State: cloneRuntimeOwnerState(committed)}, effect: cloneEffect(inFlight.effect)}
			} else {
				inFlight.command.reply <- stateOwnerResult{err: err}
				var installed statePersistenceInstalledError
				if errors.As(err, &installed) {
					poisoned = err
					for _, command := range queue {
						command.reply <- stateOwnerResult{err: err}
					}
					queue = nil
				}
			}
			inFlight, persistenceResult = nil, nil
		case <-stopInput:
			stopping = true
			for _, command := range queue {
				command.reply <- stateOwnerResult{err: errStateOwnerStopped}
			}
			queue = nil
		}
	}
}

func (o *stateOwner) close(ctx context.Context) error {
	select {
	case <-o.done:
		return nil
	default:
	}
	o.stopOnce.Do(func() { close(o.stop) })
	select {
	case <-o.done:
		return nil
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
	return o.submitWithAdmission(ctx, command, nil)
}

func (o *stateOwner) submitWithAdmission(ctx context.Context, command stateOwnerCommand, admitted chan<- struct{}) (stateOwnerResult, error) {
	if err := ctx.Err(); err != nil {
		return stateOwnerResult{}, err
	}
	command.context = ctx
	command.arbitration = &stateOwnerCommandArbitration{}
	command.reply = make(chan stateOwnerResult, 1)
	select {
	case <-o.done:
		return stateOwnerResult{}, errStateOwnerStopped
	case o.commands <- command:
	case <-ctx.Done():
		return stateOwnerResult{}, ctx.Err()
	}
	if admitted != nil {
		close(admitted)
	}
	select {
	case result := <-command.reply:
		return result, result.err
	case <-ctx.Done():
		if command.arbitration.state.CompareAndSwap(0, stateOwnerCommandCanceled) {
			return stateOwnerResult{}, ctx.Err()
		}
		result := <-command.reply
		return result, result.err
	}
}

func claimStateOwnerCommand(command stateOwnerCommand) bool {
	return command.arbitration == nil || command.arbitration.state.CompareAndSwap(0, stateOwnerCommandClaimed)
}

func commandCancellationError(command stateOwnerCommand) error {
	if command.context != nil && command.context.Err() != nil {
		return command.context.Err()
	}
	return context.Canceled
}

func (o *stateOwner) advanceIssueGeneration(ctx context.Context, command advanceIssueGenerationCommand) (stateOwnerSnapshot, error) {
	result, err := o.submit(ctx, stateOwnerCommand{kind: stateOwnerAdvanceIssueGeneration, issue: command})
	return result.snapshot, err
}

func (o *stateOwner) invalidateAttempt(ctx context.Context, command invalidateAttemptCommand) (stateOwnerSnapshot, *runtimeEffectIntent, error) {
	result, err := o.submit(ctx, stateOwnerCommand{kind: stateOwnerInvalidateAttempt, invalidate: command})
	return result.snapshot, result.effect, err
}

func (o *stateOwner) selectWorkerSeal(ctx context.Context, command selectWorkerSealCommand) (stateOwnerSnapshot, error) {
	result, err := o.submit(ctx, stateOwnerCommand{kind: stateOwnerSelectWorkerSeal, selectWorkerSeal: command})
	return result.snapshot, err
}

func (o *stateOwner) admitMachineStatus(ctx context.Context, command admitMachineStatusCommand) (stateOwnerSnapshot, error) {
	result, err := o.submit(ctx, stateOwnerCommand{kind: stateOwnerAdmitMachineStatus, machineStatus: command})
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

func (o *stateOwner) beginRuntimeEffect(ctx context.Context, command beginRuntimeEffectCommand) (stateOwnerSnapshot, *runtimeEffectIntent, error) {
	if command.Action == agentruntime.EffectStart && command.StartGateNonce == "" {
		var err error
		command.StartGateNonce, err = agentruntime.NewLaunchToken()
		if err != nil {
			return stateOwnerSnapshot{}, nil, err
		}
	}
	result, err := o.submit(ctx, stateOwnerCommand{kind: stateOwnerBeginRuntimeEffect, begin: command})
	return result.snapshot, result.effect, err
}

func (o *stateOwner) finishRuntimeEffect(ctx context.Context, command finishRuntimeEffectCommand) (stateOwnerSnapshot, error) {
	result, err := o.submit(ctx, stateOwnerCommand{kind: stateOwnerFinishRuntimeEffect, finish: command})
	return result.snapshot, err
}

func (o *stateOwner) authorizeRuntimeEffect(ctx context.Context, command authorizeRuntimeEffectCommand) error {
	_, err := o.submit(ctx, stateOwnerCommand{kind: stateOwnerAuthorizeRuntimeEffect, authorize: command})
	return err
}

func (o *stateOwner) rotateStartGate(ctx context.Context, command rotateStartGateCommand) (stateOwnerSnapshot, *runtimeEffectIntent, error) {
	var err error
	command.NewNonce, err = agentruntime.NewLaunchToken()
	if err != nil {
		return stateOwnerSnapshot{}, nil, err
	}
	result, err := o.submit(ctx, stateOwnerCommand{kind: stateOwnerRotateStartGate, rotateStartGate: command})
	return result.snapshot, result.effect, err
}

func (o *stateOwner) recordControlReceipt(ctx context.Context, receipt controlReceipt) (stateOwnerSnapshot, error) {
	result, err := o.submit(ctx, stateOwnerCommand{kind: stateOwnerRecordControlReceipt, receipt: recordControlReceiptCommand{Receipt: receipt}})
	return result.snapshot, err
}

func (o *stateOwner) recordOperatorDiagnostic(ctx context.Context, command recordOperatorDiagnosticCommand) (stateOwnerSnapshot, error) {
	result, err := o.submit(ctx, stateOwnerCommand{kind: stateOwnerRecordOperatorDiagnostic, operatorDiagnostic: command})
	return result.snapshot, err
}

func (o *stateOwner) completeHandoffCompensation(ctx context.Context, command completeHandoffCompensationCommand) (stateOwnerSnapshot, error) {
	result, err := o.submit(ctx, stateOwnerCommand{kind: stateOwnerCompleteHandoffCompensation, completeHandoff: command})
	return result.snapshot, err
}

func (o *stateOwner) beginOperatorMutation(ctx context.Context, command beginOperatorMutationCommand) (stateOwnerSnapshot, *runtimeEffectIntent, error) {
	result, err := o.submit(ctx, stateOwnerCommand{kind: stateOwnerBeginOperatorMutation, beginOperator: command})
	return result.snapshot, result.effect, err
}

func (o *stateOwner) startOperatorCleanup(ctx context.Context, command startOperatorCleanupCommand) (stateOwnerSnapshot, error) {
	result, err := o.submit(ctx, stateOwnerCommand{kind: stateOwnerStartOperatorCleanup, startOperatorCleanup: command})
	return result.snapshot, err
}

func (o *stateOwner) finishOperatorRuntimeEffect(ctx context.Context, command finishOperatorRuntimeEffectCommand) (stateOwnerSnapshot, error) {
	result, err := o.submit(ctx, stateOwnerCommand{kind: stateOwnerFinishOperatorRuntimeEffect, finishOperatorRuntime: command})
	return result.snapshot, err
}

func (o *stateOwner) finishOperatorReconciliationEffect(ctx context.Context, command finishOperatorReconciliationEffectCommand) (stateOwnerSnapshot, error) {
	result, err := o.submit(ctx, stateOwnerCommand{kind: stateOwnerFinishOperatorReconciliationEffect, finishOperatorReconcile: command})
	return result.snapshot, err
}

func (o *stateOwner) markPlanReviewRunning(ctx context.Context, command markPlanReviewRunningCommand) (stateOwnerSnapshot, error) {
	result, err := o.submit(ctx, stateOwnerCommand{kind: stateOwnerMarkPlanReviewRunning, markPlanReviewRunning: command})
	return result.snapshot, err
}

func (o *stateOwner) markReviewerSessionRequested(ctx context.Context, command markReviewerSessionRequestedCommand) (stateOwnerSnapshot, error) {
	result, err := o.submit(ctx, stateOwnerCommand{kind: stateOwnerMarkReviewerSessionRequested, markReviewerSessionRequested: command})
	return result.snapshot, err
}

func (o *stateOwner) proveReviewerDead(ctx context.Context, command proveReviewerDeadCommand) (stateOwnerSnapshot, error) {
	result, err := o.submit(ctx, stateOwnerCommand{kind: stateOwnerProveReviewerDead, proveReviewerDead: command})
	return result.snapshot, err
}

func (o *stateOwner) sealReviewerResult(ctx context.Context, command sealReviewerResultCommand) (stateOwnerSnapshot, error) {
	result, err := o.submit(ctx, stateOwnerCommand{kind: stateOwnerSealReviewerResult, sealReviewerResult: command})
	return result.snapshot, err
}

func (o *stateOwner) markReviewerStopped(ctx context.Context, command markReviewerStoppedCommand) (stateOwnerSnapshot, error) {
	result, err := o.submit(ctx, stateOwnerCommand{kind: stateOwnerMarkReviewerStopped, markReviewerStopped: command})
	return result.snapshot, err
}

func (o *stateOwner) bindReviewerStopping(ctx context.Context, command bindReviewerStoppingCommand) (stateOwnerSnapshot, error) {
	result, err := o.submit(ctx, stateOwnerCommand{kind: stateOwnerBindReviewerStopping, bindReviewerStopping: command})
	return result.snapshot, err
}

func (o *stateOwner) supersedePlanReview(ctx context.Context, command supersedePlanReviewCommand) (stateOwnerSnapshot, error) {
	result, err := o.submit(ctx, stateOwnerCommand{kind: stateOwnerSupersedePlanReview, supersedePlanReview: command})
	return result.snapshot, err
}

func (o *stateOwner) advanceOperatorRecovery(ctx context.Context, command advanceOperatorRecoveryCommand) (stateOwnerSnapshot, *runtimeEffectIntent, error) {
	result, err := o.submit(ctx, stateOwnerCommand{kind: stateOwnerAdvanceOperatorRecovery, advanceOperatorRecovery: command})
	return result.snapshot, result.effect, err
}

func applyStateOwnerCommand(attemptRoot, stateRoot string, committed runtimeOwnerState, command stateOwnerCommand, appliedCycles map[string]appliedReconciliationCycle) (runtimeOwnerState, *runtimeEffectIntent, error) {
	candidate := cloneRuntimeOwnerState(committed)
	if command.kind == stateOwnerStart {
		if candidate.Epoch == ^uint64(0) {
			return runtimeOwnerState{}, nil, errors.New("runtime epoch overflow")
		}
		if !candidate.ReviewerRevocationTracked {
			revokeInvalidPlanReviewers(&candidate, true) // Old ledgers cannot prove an A→B→A observation history.
			candidate.ReviewerRevocationTracked = true
		}
		if !candidate.ReviewerSafetyMigrated {
			migrateLegacyReviewerSafety(&candidate)
			candidate.ReviewerSafetyMigrated = true
		}
		if !candidate.ExternalDispatchTracked {
			for id, effect := range candidate.Effects {
				if effect.State == "pending" && effect.Reconciliation != nil && reconciliationMutatesGitHub(effect.Reconciliation.Action) {
					effect.Dispatched = true // A legacy pending effect may already have crossed the HTTP boundary.
					candidate.Effects[id] = effect
				}
			}
			candidate.ExternalDispatchTracked = true
		}
		if candidate.MachineStatuses == nil {
			candidate.MachineStatuses = map[string]machineStatusRecord{}
		}
		for id, effect := range candidate.Effects {
			if effect.Action == string(agentruntime.EffectStop) && effect.State == "pending" {
				key := ownerAttemptKey(effect.Repository, effect.Issue, effect.Attempt)
				if record, ok := candidate.Attempts[key]; ok && record.Generation == effect.AttemptGeneration && record.StopEffectID == "" {
					record.StopEffectID = id
					candidate.Attempts[key] = record
				}
			}
		}
		candidate.Epoch++
		return finishRuntimeOwnerTransition(attemptRoot, stateRoot, candidate, nil)
	}
	switch command.kind {
	case stateOwnerAdvanceIssueGeneration:
		if err := applyAdvanceIssueGeneration(&candidate, command.issue); err != nil {
			return runtimeOwnerState{}, nil, err
		}
	case stateOwnerInvalidateAttempt:
		effect, err := applyInvalidateAttempt(attemptRoot, stateRoot, &candidate, command.invalidate)
		if err != nil {
			return runtimeOwnerState{}, nil, err
		}
		if reflect.DeepEqual(candidate, committed) {
			return candidate, effect, nil
		}
		return finishRuntimeOwnerTransition(attemptRoot, stateRoot, candidate, effect)
	case stateOwnerRecordEffect:
		effect, err := applyRecordEffect(&candidate, command.record)
		if err != nil {
			return runtimeOwnerState{}, nil, err
		}
		return finishRuntimeOwnerTransition(attemptRoot, stateRoot, candidate, effect)
	case stateOwnerCompleteEffect:
		if err := applyCompleteEffect(&candidate, command.complete); err != nil {
			return runtimeOwnerState{}, nil, err
		}
	case stateOwnerBeginRuntimeEffect:
		effect, err := applyBeginRuntimeEffect(attemptRoot, stateRoot, &candidate, command.begin)
		if err != nil {
			return runtimeOwnerState{}, nil, err
		}
		if effect.ID != "" {
			return candidate, cloneEffect(effect), nil
		}
		return finishRuntimeOwnerTransition(attemptRoot, stateRoot, candidate, effect)
	case stateOwnerFinishRuntimeEffect:
		if err := applyFinishRuntimeEffect(attemptRoot, stateRoot, &candidate, command.finish); err != nil {
			return runtimeOwnerState{}, nil, err
		}
	case stateOwnerAuthorizeRuntimeEffect:
		if err := applyAuthorizeRuntimeEffect(&candidate, command.authorize); err != nil {
			return runtimeOwnerState{}, nil, err
		}
	case stateOwnerRotateStartGate:
		if err := applyRotateStartGate(&candidate, command.rotateStartGate); err != nil {
			return runtimeOwnerState{}, nil, err
		}
		effect := candidate.Effects[command.rotateStartGate.Identity.EffectID]
		return finishRuntimeOwnerTransition(attemptRoot, stateRoot, candidate, &effect)
	case stateOwnerDiagnoseRuntimeEffect:
		if err := applyDiagnoseRuntimeEffect(&candidate, command.diagnoseRuntime); err != nil {
			return runtimeOwnerState{}, nil, err
		}
	case stateOwnerApplyReconciliation:
		if err := applyReconciliation(&candidate, command.reconcile, appliedCycles); err != nil {
			return runtimeOwnerState{}, nil, err
		}
	case stateOwnerBeginReconciliationEffect:
		effect, err := applyBeginReconciliationEffect(attemptRoot, stateRoot, &candidate, command.beginReconciliation)
		if err != nil {
			return runtimeOwnerState{}, nil, err
		}
		if effect.ID != "" {
			return candidate, cloneEffect(effect), nil
		}
		return finishRuntimeOwnerTransition(attemptRoot, stateRoot, candidate, effect)
	case stateOwnerAuthorizeReconciliationEffect:
		if err := applyAuthorizeReconciliationEffect(stateRoot, &candidate, command.authorizeReconciliation); err != nil {
			return runtimeOwnerState{}, nil, err
		}
	case stateOwnerResolveInvalidatedReconciliationEffect:
		if err := applyResolveInvalidatedReconciliationEffect(&candidate, command.resolveInvalidatedReconcile); err != nil {
			return runtimeOwnerState{}, nil, err
		}
	case stateOwnerSelectWorkerSeal:
		if err := applySelectWorkerSeal(stateRoot, &candidate, command.selectWorkerSeal); err != nil {
			return runtimeOwnerState{}, nil, err
		}
	case stateOwnerAdmitMachineStatus:
		if err := applyAdmitMachineStatus(&candidate, command.machineStatus); err != nil {
			return runtimeOwnerState{}, nil, err
		}
	case stateOwnerMarkPlanReviewRunning:
		if err := applyMarkPlanReviewRunning(stateRoot, &candidate, command.markPlanReviewRunning); err != nil {
			return runtimeOwnerState{}, nil, err
		}
	case stateOwnerMarkReviewerSessionRequested:
		if err := applyMarkReviewerSessionRequested(stateRoot, &candidate, command.markReviewerSessionRequested); err != nil {
			return runtimeOwnerState{}, nil, err
		}
	case stateOwnerProveReviewerDead:
		if err := applyProveReviewerDead(&candidate, command.proveReviewerDead); err != nil {
			return runtimeOwnerState{}, nil, err
		}
	case stateOwnerSealReviewerResult:
		if err := applySealReviewerResult(stateRoot, &candidate, command.sealReviewerResult); err != nil {
			return runtimeOwnerState{}, nil, err
		}
	case stateOwnerMarkReviewerStopped:
		if err := applyMarkReviewerStopped(&candidate, command.markReviewerStopped); err != nil {
			return runtimeOwnerState{}, nil, err
		}
	case stateOwnerBindReviewerStopping:
		if err := applyBindReviewerStopping(&candidate, command.bindReviewerStopping); err != nil {
			return runtimeOwnerState{}, nil, err
		}
	case stateOwnerSupersedePlanReview:
		if err := applySupersedePlanReview(&candidate, command.supersedePlanReview); err != nil {
			return runtimeOwnerState{}, nil, err
		}
	case stateOwnerFinishReconciliationEffect:
		if err := applyFinishReconciliationEffect(stateRoot, &candidate, command.finishReconciliation); err != nil {
			return runtimeOwnerState{}, nil, err
		}
	case stateOwnerDiagnoseReconciliationEffect:
		if err := applyDiagnoseReconciliationEffect(&candidate, command.diagnoseReconciliation); err != nil {
			return runtimeOwnerState{}, nil, err
		}
	case stateOwnerMutatePRRecovery:
		if err := applyMutatePRRecovery(stateRoot, &candidate, command.mutatePRRecovery); err != nil {
			return runtimeOwnerState{}, nil, err
		}
	case stateOwnerMutateGovernancePhase:
		if err := applyMutateGovernancePhase(stateRoot, &candidate, command.mutateGovernancePhase); err != nil {
			return runtimeOwnerState{}, nil, err
		}
	case stateOwnerRecordControlReceipt:
		if err := applyControlReceipt(&candidate, command.receipt.Receipt); err != nil {
			return runtimeOwnerState{}, nil, err
		}
	case stateOwnerRecordOperatorDiagnostic:
		if err := applyRecordOperatorDiagnostic(&candidate, command.operatorDiagnostic); err != nil {
			return runtimeOwnerState{}, nil, err
		}
	case stateOwnerCompleteHandoffCompensation:
		if err := applyCompleteHandoffCompensation(&candidate, command.completeHandoff); err != nil {
			return runtimeOwnerState{}, nil, err
		}
	case stateOwnerBeginOperatorMutation:
		effect, err := applyBeginOperatorMutation(attemptRoot, stateRoot, &candidate, command.beginOperator)
		if err != nil {
			return runtimeOwnerState{}, nil, err
		}
		if reflect.DeepEqual(candidate, committed) {
			return candidate, cloneEffect(effect), nil
		}
		return finishRuntimeOwnerTransition(attemptRoot, stateRoot, candidate, effect, command.beginOperator.Request.RequestID)
	case stateOwnerStartOperatorCleanup:
		if err := applyStartOperatorCleanup(&candidate, command.startOperatorCleanup); err != nil {
			return runtimeOwnerState{}, nil, err
		}
	case stateOwnerFinishOperatorRuntimeEffect:
		if err := applyFinishOperatorRuntimeEffect(attemptRoot, stateRoot, &candidate, command.finishOperatorRuntime); err != nil {
			return runtimeOwnerState{}, nil, err
		}
	case stateOwnerFinishOperatorReconciliationEffect:
		if err := applyFinishOperatorReconciliationEffect(stateRoot, &candidate, command.finishOperatorReconcile); err != nil {
			return runtimeOwnerState{}, nil, err
		}
	case stateOwnerAdvanceOperatorRecovery:
		effect, err := applyAdvanceOperatorRecovery(attemptRoot, stateRoot, &candidate, command.advanceOperatorRecovery)
		if err != nil {
			return runtimeOwnerState{}, nil, err
		}
		requestIDs := []string{}
		for _, receipt := range candidate.ControlReceipts {
			if receipt.Request.Action == "recover" && receipt.State == "pending" && receipt.EffectID == "" && (receipt.Phase == operatorPhaseTerminal || receipt.Phase == operatorPhaseRetryPending) {
				requestIDs = append(requestIDs, receipt.Request.RequestID)
			}
		}
		return finishRuntimeOwnerTransition(attemptRoot, stateRoot, candidate, effect, requestIDs...)
	case stateOwnerRecordCycleOutcome:
		outcome := command.cycleOutcome
		if outcome.Identity.Epoch != candidate.Epoch || outcome.Identity.CycleID == 0 || outcome.Identity.SourceRevision == 0 || outcome.Identity.SourceRevision > candidate.Revision || !boundedText(outcome.Diagnostic, maxReconciliationStringBytes, false) || (outcome.Diagnostic == "") != outcome.At.IsZero() {
			return runtimeOwnerState{}, nil, errStateConflict
		}
		if candidate.CycleOutcomeEpoch == outcome.Identity.Epoch && outcome.Identity.CycleID < candidate.CycleOutcomeID {
			return runtimeOwnerState{}, nil, errStaleStateResult
		}
		if candidate.CycleOutcomeEpoch == outcome.Identity.Epoch && outcome.Identity.CycleID == candidate.CycleOutcomeID {
			if candidate.CycleOutcomeSource == outcome.Identity.SourceRevision && candidate.CycleDiagnostic == outcome.Diagnostic && candidate.CycleDiagnosticAt.Equal(outcome.At) {
				return candidate, nil, nil
			}
			return runtimeOwnerState{}, nil, errStateConflict
		}
		candidate.CycleDiagnostic = outcome.Diagnostic
		candidate.CycleDiagnosticAt = outcome.At.UTC()
		candidate.CycleOutcomeEpoch = outcome.Identity.Epoch
		candidate.CycleOutcomeID = outcome.Identity.CycleID
		candidate.CycleOutcomeSource = outcome.Identity.SourceRevision
	default:
		return runtimeOwnerState{}, nil, errors.New("unknown state owner command")
	}
	if reflect.DeepEqual(candidate, committed) {
		return candidate, nil, nil
	}
	return finishRuntimeOwnerTransition(attemptRoot, stateRoot, candidate, nil)
}

func applyAuthorizeRuntimeEffect(state *runtimeOwnerState, command authorizeRuntimeEffectCommand) error {
	identity := command.Identity
	if !validRuntimeEffectAction(command.Action) || identity.EffectID == "" || identity.SourceRevision == 0 || !agentruntime.ValidEffectRequestDigest(identity.RequestDigest) {
		return errStaleStateResult
	}
	effect, ok := state.Effects[identity.EffectID]
	if !ok || effect.State != "pending" || effect.Action != string(command.Action) || effect.IntentEpoch != identity.Epoch || effect.IntentRevision != identity.SourceRevision || effect.IssueGeneration != identity.IssueGeneration || effect.AttemptGeneration != identity.AttemptGeneration || effect.RequestDigest != identity.RequestDigest {
		return errStaleStateResult
	}
	issueKey := ownerIssueKey(effect.Repository, effect.Issue)
	attemptKey := ownerAttemptKey(effect.Repository, effect.Issue, effect.Attempt)
	if state.AttemptGenerations[attemptKey] != identity.AttemptGeneration || state.IssueGenerations[issueKey] != identity.IssueGeneration && !effectAuthorizedByTombstone(*state, effect) {
		return errStaleStateResult
	}
	if _, tombstoned := state.Tombstones[attemptKey]; tombstoned && !effectAuthorizedByTombstone(*state, effect) {
		return errAttemptTombstoned
	}
	if command.Action == agentruntime.EffectStart {
		if !agentruntime.ValidLaunchToken(command.GateNonce) || effect.StartGateNonce != command.GateNonce || len(effect.StartCandidates) == 0 || effect.StartCandidates[len(effect.StartCandidates)-1].Nonce != command.GateNonce {
			return errStaleStateResult
		}
		effect.StartMayRun = true
		effect.StartCandidates[len(effect.StartCandidates)-1].MayRun = true
		state.Effects[effect.ID] = effect
	} else if command.GateNonce != "" {
		return errStateConflict
	}
	return nil
}

func applyRotateStartGate(state *runtimeOwnerState, command rotateStartGateCommand) error {
	identity := command.Identity
	effect, ok := state.Effects[identity.EffectID]
	if !ok || effect.State != "pending" || effect.Action != string(agentruntime.EffectStart) || effect.IntentEpoch != identity.Epoch || effect.IntentRevision != identity.SourceRevision || effect.IssueGeneration != identity.IssueGeneration || effect.AttemptGeneration != identity.AttemptGeneration || effect.RequestDigest != identity.RequestDigest || effect.StartMayRun || effect.StartGateNonce != command.OldNonce || !agentruntime.ValidLaunchToken(command.NewNonce) || command.NewNonce == command.OldNonce || !validStartCandidates(effect.StartCandidates) {
		return errStaleStateResult
	}
	issueKey, attemptKey := ownerIssueKey(effect.Repository, effect.Issue), ownerAttemptKey(effect.Repository, effect.Issue, effect.Attempt)
	if state.IssueGenerations[issueKey] != identity.IssueGeneration || state.AttemptGenerations[attemptKey] != identity.AttemptGeneration {
		return errStaleStateResult
	}
	if _, tombstoned := state.Tombstones[attemptKey]; tombstoned {
		return errAttemptTombstoned
	}
	for _, candidate := range effect.StartCandidates {
		if candidate.Nonce == command.NewNonce || candidate.MayRun {
			return errStateConflict
		}
	}
	effect.StartGateNonce = command.NewNonce
	effect.StartCandidates = append(effect.StartCandidates, startGateCandidate{Nonce: command.NewNonce})
	state.Effects[effect.ID] = effect
	return nil
}

func applyBeginRuntimeEffect(attemptRoot, stateRoot string, state *runtimeOwnerState, command beginRuntimeEffectCommand) (*runtimeEffectIntent, error) {
	manifest, identity := cloneManifest(command.Manifest), command.Identity
	// Handoff launches are committed by the reconciliation handoff intent.
	// There is no production runtime-Handoff producer or restart scheduler.
	if command.Action == agentruntime.EffectHandoff || command.CandidateLaunchToken != "" || command.Action == agentruntime.EffectStart && !agentruntime.ValidLaunchToken(command.StartGateNonce) || command.Action != agentruntime.EffectStart && command.StartGateNonce != "" {
		return nil, errStateConflict
	}
	if !validRuntimeEffectInput(command.Action, command.Reason) || !agentruntime.ValidEffectRequestDigest(command.RequestDigest) || command.Action == agentruntime.EffectCleanup || command.Action != agentruntime.EffectStop && command.SupersededReviewerID != "" || identity.Epoch != state.Epoch || identity.EffectID != "" {
		return nil, errStaleStateResult
	}
	if err := validateOwnerManifest(state.Repository, attemptRoot, stateRoot, manifest); err != nil {
		return nil, err
	}
	if !validOwnerRuntimeEffectInput(attemptRoot, stateRoot, command.Action, manifest, command.Reason, command.Review) {
		return nil, errStateConflict
	}
	issueKey, attemptKey := ownerIssueKey(manifest.Repository, manifest.Issue), ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)
	if record := state.Attempts[attemptKey]; record.StopEffectID != "" && command.Action != agentruntime.EffectStop {
		return nil, errStateConflict
	}
	if command.Action != agentruntime.EffectStop && issueHasUnprovedReviewer(*state, manifest.Repository, manifest.Issue) {
		return nil, errStateConflict
	}
	if command.Action == agentruntime.EffectStop {
		for _, effect := range state.Effects {
			record, exists := state.Attempts[attemptKey]
			if effect.Repository == manifest.Repository && effect.Issue == manifest.Issue && effect.Attempt == manifest.Attempt && effect.State == "pending" && effect.Action == string(agentruntime.EffectStop) {
				if exists && record.Generation == effect.AttemptGeneration && reflect.DeepEqual(record.Manifest, manifest) && effect.IssueGeneration == state.IssueGenerations[issueKey] && effect.RequestDigest == command.RequestDigest && effect.Reason == command.Reason && effect.SupersededReviewerID == command.SupersededReviewerID {
					return cloneEffect(&effect), nil
				}
				return nil, errStateConflict
			}
		}
	}
	if identity.SourceRevision != state.Revision {
		return nil, errStaleStateResult
	}
	if state.IssueGenerations[issueKey] != identity.IssueGeneration || state.AttemptGenerations[attemptKey] != identity.AttemptGeneration {
		return nil, errStaleStateResult
	}
	if _, tombstoned := state.Tombstones[attemptKey]; tombstoned {
		return nil, errAttemptTombstoned
	}
	for _, effect := range state.Effects {
		if effect.Repository == manifest.Repository && effect.Issue == manifest.Issue && effect.Attempt == manifest.Attempt && effect.State == "pending" && command.Action != agentruntime.EffectStop {
			return nil, errStateConflict
		}
	}
	var supersededReviewer runtimeEffectIntent
	var invalidatedHandoff *handoffCandidateInvalidation
	var invalidatedStart *startCandidateInvalidation
	if command.Action == agentruntime.EffectStop {
		var err error
		invalidatedHandoff, err = pendingHandoffCandidate(*state, manifest.Repository, manifest.Issue, manifest.Attempt)
		if err != nil {
			return nil, err
		}
		invalidatedStart, err = pendingStartCandidate(*state, manifest)
		if err != nil {
			return nil, err
		}
	}
	switch command.Action {
	case agentruntime.EffectPrepare:
		if _, exists := state.Attempts[attemptKey]; exists || identity.AttemptGeneration != 0 || hasAttemptEffect(*state, manifest.Repository, manifest.Issue, manifest.Attempt) {
			return nil, errStateConflict
		}
		if err := applyUpsertAttempt(attemptRoot, stateRoot, state, upsertAttemptCommand{Manifest: manifest, ExpectedIssueGeneration: identity.IssueGeneration, ExpectedAttemptGeneration: 0}); err != nil {
			return nil, err
		}
		identity.IssueGeneration = state.IssueGenerations[issueKey]
		identity.AttemptGeneration = state.AttemptGenerations[attemptKey]
	case agentruntime.EffectStop:
		record, exists := state.Attempts[attemptKey]
		if !exists || !reflect.DeepEqual(record.Manifest, manifest) {
			return nil, errStateConflict
		}
		var reviewerErr error
		supersededReviewer, reviewerErr = pendingReviewerEffect(*state, manifest.Repository, manifest.Issue, manifest.Attempt)
		if reviewerErr != nil || supersededReviewer.ID != command.SupersededReviewerID {
			return nil, errStateConflict
		}
		if err := applyUpsertAttemptAllowingReviewer(attemptRoot, stateRoot, state, upsertAttemptCommand{Manifest: manifest, ExpectedIssueGeneration: identity.IssueGeneration, ExpectedAttemptGeneration: identity.AttemptGeneration}, supersededReviewer.ID); err != nil {
			return nil, err
		}
		identity.IssueGeneration = state.IssueGenerations[issueKey]
		identity.AttemptGeneration = state.AttemptGenerations[attemptKey]
	default:
		record, exists := state.Attempts[attemptKey]
		if !exists || record.Generation != identity.AttemptGeneration || !reflect.DeepEqual(record.Manifest, manifest) {
			return nil, errStaleStateResult
		}
		pruneCompletedAttemptEffects(state, manifest.Repository, manifest.Issue, manifest.Attempt)
	}
	effect := &runtimeEffectIntent{Action: string(command.Action), Repository: manifest.Repository, Issue: manifest.Issue, Attempt: manifest.Attempt, IssueGeneration: identity.IssueGeneration, AttemptGeneration: identity.AttemptGeneration, IntentEpoch: state.Epoch, State: "pending", RequestDigest: command.RequestDigest, Reason: command.Reason, Review: cloneReviewTransition(command.Review), CandidateLaunchToken: command.CandidateLaunchToken, InvalidatedHandoff: invalidatedHandoff, InvalidatedStart: invalidatedStart}
	if command.Action == agentruntime.EffectMonitor {
		effect.MachineStatusSequence = state.MachineStatuses[issueKey].Sequence
	}
	if command.Action == agentruntime.EffectStart {
		effect.StartGateNonce = command.StartGateNonce
		effect.StartCandidates = []startGateCandidate{{Nonce: command.StartGateNonce}}
	}
	bindSupersededReviewer(effect, supersededReviewer)
	return effect, nil
}

func applyFinishRuntimeEffect(attemptRoot, stateRoot string, state *runtimeOwnerState, command finishRuntimeEffectCommand) error {
	identity := command.Identity
	if !validRuntimeEffectAction(command.Action) || identity.EffectID == "" || identity.SourceRevision == 0 {
		return errStaleStateResult
	}
	effect, ok := state.Effects[identity.EffectID]
	if !ok || effect.Action != string(command.Action) || effect.IntentEpoch != identity.Epoch || effect.IntentRevision != identity.SourceRevision || effect.IssueGeneration != identity.IssueGeneration || effect.AttemptGeneration != identity.AttemptGeneration || effect.RequestDigest != identity.RequestDigest {
		return errStaleStateResult
	}
	if effect.SupersededReviewerID != "" && !effect.ReviewerStopped {
		return errStateConflict
	}
	if (command.Action == agentruntime.EffectCleanup || command.Action == agentruntime.EffectStop) && (attemptHasUnprovedReviewer(*state, effect.Repository, effect.Issue, effect.Attempt) || state.LegacyReviewerQuarantines[ownerIssueKey(effect.Repository, effect.Issue)] != "") {
		return errStateConflict
	}
	issueKey, attemptKey := ownerIssueKey(effect.Repository, effect.Issue), ownerAttemptKey(effect.Repository, effect.Issue, effect.Attempt)
	if state.IssueGenerations[issueKey] != identity.IssueGeneration && !effectAuthorizedByTombstone(*state, effect) || state.AttemptGenerations[attemptKey] != identity.AttemptGeneration {
		return errStaleStateResult
	}
	manifest := cloneManifest(command.Manifest)
	if err := validateOwnerManifest(state.Repository, attemptRoot, stateRoot, manifest); err != nil || manifest.Repository != effect.Repository || manifest.Issue != effect.Issue || manifest.Attempt != effect.Attempt {
		return errStateConflict
	}
	if command.Action == agentruntime.EffectStart && manifest.State == "running" && (!effect.StartMayRun || !validStartCandidates(effect.StartCandidates)) {
		return errStateConflict
	}
	if runtimeEffectAppliesManifest(command.Action) {
		if _, tombstoned := state.Tombstones[attemptKey]; tombstoned {
			return errAttemptTombstoned
		}
		record, exists := state.Attempts[attemptKey]
		if !exists || record.Generation != identity.AttemptGeneration {
			return errStaleStateResult
		}
		if command.Action == agentruntime.EffectReview {
			if effect.Review == nil {
				return errStateConflict
			}
			if _, err := agentruntime.ReviewEffectResult(attemptRoot, stateRoot, record.Manifest, *effect.Review); err != nil {
				return errStateConflict
			}
		}
		if !validRuntimeEffectResult(command.Action, effect, record.Manifest, manifest) {
			return errStateConflict
		}
		if effect.State == "completed" {
			if reflect.DeepEqual(record.Manifest, manifest) && effect.Diagnostic == manifest.Diagnostic {
				return nil
			}
			return errStateConflict
		}
		previousWorkerSequence := record.Manifest.WorkerStatusSeq
		record.Manifest = manifest
		if command.Action == agentruntime.EffectStop {
			// The stop generation invalidates the launch-bound worker authority.
			record.Manifest.WorkerGeneration = 0
			record.Manifest.WorkerProfileDigest = ""
			if record.StopEffectID != "" && record.StopEffectID != effect.ID {
				return errStateConflict
			}
			record.StopEffectID = ""
		}
		state.Attempts[attemptKey] = record
		if command.Action == agentruntime.EffectMonitor && manifest.WorkerStatusSeq > previousWorkerSequence {
			if err := applyAdmitMachineStatus(state, admitMachineStatusCommand{
				Repository: manifest.Repository, Issue: manifest.Issue, Attempt: manifest.Attempt,
				ExpectedIssueGeneration: identity.IssueGeneration, ExpectedAttemptGeneration: identity.AttemptGeneration,
				ExpectedStatusSequence: effect.MachineStatusSequence,
				Source:                 "worker", SourceID: fmt.Sprintf("%d:%d", identity.AttemptGeneration, manifest.WorkerStatusSeq), Status: manifest.WorkerStatus, Reason: manifest.WorkerStatusReason,
			}); err != nil {
				return err
			}
		}
	} else {
		stored := manifest
		if tombstone, ok := state.Tombstones[attemptKey]; ok && tombstone.Manifest != nil {
			stored = *tombstone.Manifest
		} else if record, ok := state.Attempts[attemptKey]; ok {
			stored = record.Manifest
		}
		if !reflect.DeepEqual(stored, manifest) {
			return errStateConflict
		}
	}
	if !runtimeEffectAppliesManifest(command.Action) && effect.State == "completed" {
		if effect.Diagnostic == manifest.Diagnostic {
			return nil
		}
		return errStateConflict
	}
	effect.State, effect.Diagnostic = "completed", manifest.Diagnostic
	state.Effects[effect.ID] = effect
	if command.Action == agentruntime.EffectCleanup {
		for key, proof := range state.ReviewerProofs {
			if proof.Repository == effect.Repository && proof.Issue == effect.Issue && proof.Attempt == effect.Attempt {
				delete(state.ReviewerProofs, key)
			}
		}
	}
	if tombstone, ok := state.Tombstones[attemptKey]; ok && tombstone.EffectID == effect.ID {
		tombstone.CleanupPhase = "completed"
		state.Tombstones[attemptKey] = tombstone
	}
	return nil
}

func applyMarkReviewerStopped(state *runtimeOwnerState, command markReviewerStoppedCommand) error {
	// A stopped tmux session or vanished process group is not proof that a
	// launched reviewer's detached descendants are gone.
	if !command.Observation.NeverRan {
		return errStateConflict
	}
	effect, ok := state.Effects[command.Identity.EffectID]
	if !ok || effect.State != "pending" || effect.SupersededReviewerID == "" || effect.Action != string(agentruntime.EffectStop) && effect.Action != string(agentruntime.EffectCleanup) || effect.IntentEpoch != command.Identity.Epoch || effect.IntentRevision != command.Identity.SourceRevision || effect.IssueGeneration != command.Identity.IssueGeneration || effect.AttemptGeneration != command.Identity.AttemptGeneration || effect.RequestDigest != command.Identity.RequestDigest {
		return errStaleStateResult
	}
	if effect.ReviewerStopped {
		return nil
	}
	key := reviewerProofKey(effect.Repository, effect.Issue, effect.Attempt, effect.SupersededReviewerMode, effect.SupersededReviewerTarget)
	observed := command.Observation
	if observed.NeverRan != (observed.GroupPID == 0) || observed.NeverRan && (!effect.SupersededReviewerGateProtocol || effect.SupersededReviewerSessionRequested) || effect.SupersededReviewerGroupPID > 0 && (observed.GroupPID != effect.SupersededReviewerGroupPID || observed.NeverRan) || effect.SupersededReviewerGroupPID == 0 && !observed.NeverRan {
		return errStateConflict
	}
	if effect.SupersededReviewerGroupPID > 0 {
		proof, exists := state.ReviewerProofs[key]
		if !exists || proof.EffectID != effect.SupersededReviewerID || proof.GroupPID != effect.SupersededReviewerGroupPID || proof.IssueGeneration != effect.SupersededReviewerIssueGeneration || proof.AttemptGeneration != effect.SupersededReviewerAttemptGeneration {
			return errStateConflict
		}
		proof.DeadProved = true
		state.ReviewerProofs[key] = proof
	} else {
		if state.ReviewerProofs == nil {
			state.ReviewerProofs = map[string]reviewerProcessProof{}
		}
		state.ReviewerProofs[key] = reviewerProcessProof{Repository: effect.Repository, Issue: effect.Issue, Attempt: effect.Attempt, Mode: effect.SupersededReviewerMode, Target: effect.SupersededReviewerTarget, EffectID: effect.SupersededReviewerID, IssueGeneration: effect.SupersededReviewerIssueGeneration, AttemptGeneration: effect.SupersededReviewerAttemptGeneration, GroupPID: observed.GroupPID, DeadProved: true, NeverRan: observed.NeverRan}
	}
	effect.ReviewerStopped = true
	state.Effects[effect.ID] = effect
	return nil
}

// Bind the externally verified live group before signalling it. This is not
// death proof: a crash after the signal can replay from the durable group ID.
func applyBindReviewerStopping(state *runtimeOwnerState, command bindReviewerStoppingCommand) error {
	effect, ok := state.Effects[command.Identity.EffectID]
	if !ok || effect.State != "pending" || command.GroupPID < 2 || effect.IntentEpoch != command.Identity.Epoch || effect.IntentRevision != command.Identity.SourceRevision || effect.IssueGeneration != command.Identity.IssueGeneration || effect.AttemptGeneration != command.Identity.AttemptGeneration || effect.RequestDigest != command.Identity.RequestDigest {
		return errStaleStateResult
	}
	var proof reviewerProcessProof
	if effect.Reconciliation != nil && effect.Reconciliation.Action == reconciliationReviewer && effect.Reconciliation.Reviewer != nil && effect.Reconciliation.Reviewer.Phase == "run-observe" {
		if !effect.ReviewerGateProtocol || !effect.ReviewerSessionRequested || effect.ReviewerGroupPID != 0 && effect.ReviewerGroupPID != command.GroupPID {
			return errStateConflict
		}
		proof = reviewerProcessProof{Repository: effect.Repository, Issue: effect.Issue, Attempt: effect.Attempt, Mode: effect.Reconciliation.Reviewer.Mode, Target: effect.Reconciliation.Reviewer.Target, EffectID: effect.ID, IssueGeneration: effect.IssueGeneration, AttemptGeneration: effect.AttemptGeneration, GroupPID: command.GroupPID}
		effect.ReviewerGroupPID = command.GroupPID
		effect.ReviewerLaunched = true
	} else if effect.SupersededReviewerID != "" && (effect.Action == string(agentruntime.EffectStop) || effect.Action == string(agentruntime.EffectCleanup)) {
		if !effect.SupersededReviewerGateProtocol || !effect.SupersededReviewerSessionRequested || effect.SupersededReviewerGroupPID != 0 && effect.SupersededReviewerGroupPID != command.GroupPID {
			return errStateConflict
		}
		proof = reviewerProcessProof{Repository: effect.Repository, Issue: effect.Issue, Attempt: effect.Attempt, Mode: effect.SupersededReviewerMode, Target: effect.SupersededReviewerTarget, EffectID: effect.SupersededReviewerID, IssueGeneration: effect.SupersededReviewerIssueGeneration, AttemptGeneration: effect.SupersededReviewerAttemptGeneration, GroupPID: command.GroupPID}
		effect.SupersededReviewerGroupPID = command.GroupPID
	} else {
		return errStateConflict
	}
	key := reviewerProofKey(proof.Repository, proof.Issue, proof.Attempt, proof.Mode, proof.Target)
	if old, exists := state.ReviewerProofs[key]; exists {
		if old.EffectID == proof.EffectID && old.GroupPID == proof.GroupPID && old.IssueGeneration == proof.IssueGeneration && old.AttemptGeneration == proof.AttemptGeneration {
			proof = old
		} else if !old.DeadProved || old.EffectID == proof.EffectID || old.Repository != proof.Repository || old.Issue != proof.Issue || old.Attempt != proof.Attempt || old.Mode != proof.Mode || old.Target != proof.Target || old.IssueGeneration > proof.IssueGeneration || old.AttemptGeneration > proof.AttemptGeneration {
			return errStateConflict
		}
	}
	if state.ReviewerProofs == nil {
		state.ReviewerProofs = map[string]reviewerProcessProof{}
	}
	state.ReviewerProofs[key] = proof
	state.Effects[effect.ID] = effect
	return nil
}

func validRuntimeEffectResult(action agentruntime.EffectAction, effect runtimeEffectIntent, current, result agentruntime.Manifest) bool {
	switch action {
	case agentruntime.EffectStart:
		if result.LaunchToken != current.LaunchToken || result.LaunchID != current.LaunchID && result.LaunchID != effect.StartGateNonce || result.State == "running" && result.LaunchID != effect.StartGateNonce {
			return false
		}
	case agentruntime.EffectHandoff:
		unchanged := result.LaunchToken == current.LaunchToken && result.LaunchID == current.LaunchID
		relaunched := result.LaunchToken == effect.CandidateLaunchToken && result.LaunchID == effect.ID && agentruntime.ValidLaunchToken(effect.CandidateLaunchToken)
		if !unchanged && !relaunched {
			return false
		}
	}
	if !sameRuntimeEffectManifestBase(current, result, action) {
		return false
	}
	switch action {
	case agentruntime.EffectPrepare:
		return result.State == "preparing" && result.Diagnostic == current.Diagnostic || result.State == "failed" && result.Diagnostic != ""
	case agentruntime.EffectStart, agentruntime.EffectHandoff:
		return result.State == "running" && result.Diagnostic == "" || action == agentruntime.EffectStart && result.State == "failed" && result.Diagnostic != ""
	case agentruntime.EffectStop:
		return result.State == "cancelled" && result.Diagnostic == effect.Reason
	case agentruntime.EffectMonitor:
		return result.State == "running" && result.Diagnostic == current.Diagnostic || result.State == "completed" && result.Diagnostic == "" || result.State == "failed" && result.Diagnostic != ""
	case agentruntime.EffectReview:
		return effect.Review != nil && reviewTransitionMatches(*effect.Review, current, result)
	default:
		return false
	}
}

func validOwnerRuntimeEffectInput(attemptRoot, stateRoot string, action agentruntime.EffectAction, manifest agentruntime.Manifest, reason string, review *agentruntime.ReviewTransition) bool {
	if !validRuntimeEffectInput(action, reason) || (action == agentruntime.EffectReview) != (review != nil) {
		return false
	}
	switch action {
	case agentruntime.EffectPrepare, agentruntime.EffectStart:
		return manifest.State == "preparing"
	case agentruntime.EffectMonitor:
		return manifest.State == "running"
	case agentruntime.EffectStop:
		return manifest.State == "preparing" || manifest.State == "running"
	case agentruntime.EffectReview:
		_, err := agentruntime.ReviewEffectResult(attemptRoot, stateRoot, manifest, *review)
		return err == nil
	case agentruntime.EffectHandoff:
		return manifest.State == "completed" || manifest.State == "running"
	default:
		return false
	}
}

func sameRuntimeEffectManifestBase(current, result agentruntime.Manifest, action agentruntime.EffectAction) bool {
	copy := cloneManifest(result)
	copy.State, copy.Diagnostic, copy.UpdatedAt = current.State, current.Diagnostic, current.UpdatedAt
	if action == agentruntime.EffectStart || action == agentruntime.EffectHandoff {
		copy.LaunchToken, copy.LaunchID = current.LaunchToken, current.LaunchID
	}
	if action == agentruntime.EffectReview {
		copy.ReviewState, copy.ReviewMode, copy.ReviewTarget = current.ReviewState, current.ReviewMode, current.ReviewTarget
		copy.ReviewDiagnostic = current.ReviewDiagnostic
		copy.ReviewBase, copy.ReviewHead, copy.ReviewSnapshot, copy.ReviewSession = current.ReviewBase, current.ReviewHead, current.ReviewSnapshot, current.ReviewSession
		copy.ReviewFindings = slices.Clone(current.ReviewFindings)
		copy.ReviewHandoffQueued, copy.ReviewHandoffAck = current.ReviewHandoffQueued, current.ReviewHandoffAck
	}
	if action == agentruntime.EffectMonitor {
		copy.WorkerStatus, copy.WorkerStatusReason, copy.WorkerStatusSeq, copy.WorkerStatusApplied = current.WorkerStatus, current.WorkerStatusReason, current.WorkerStatusSeq, current.WorkerStatusApplied
	}
	if !reflect.DeepEqual(copy, current) {
		return false
	}
	if action != agentruntime.EffectMonitor {
		return result.WorkerStatus == current.WorkerStatus && result.WorkerStatusReason == current.WorkerStatusReason && result.WorkerStatusSeq == current.WorkerStatusSeq && result.WorkerStatusApplied == current.WorkerStatusApplied
	}
	return result.WorkerStatusApplied == current.WorkerStatusApplied && result.WorkerStatusSeq >= current.WorkerStatusSeq && (result.WorkerStatusSeq == current.WorkerStatusSeq && result.WorkerStatus == current.WorkerStatus && result.WorkerStatusReason == current.WorkerStatusReason || result.WorkerStatusSeq > current.WorkerStatusSeq && slices.Contains([]string{"needs-attention", "clear"}, result.WorkerStatus) && strings.TrimSpace(result.WorkerStatusReason) != "" && len(result.WorkerStatusReason) <= 1024)
}

func reviewTransitionMatches(review agentruntime.ReviewTransition, current, result agentruntime.Manifest) bool {
	wantState, wantDiagnostic := current.State, current.Diagnostic
	if review.HandoffAcknowledged && current.State == "completed" {
		wantState, wantDiagnostic = "running", ""
	}
	wantFindings := review.Findings
	wantQueued, wantAcknowledged := review.HandoffQueued, review.HandoffAcknowledged
	if review.State != "findings-queued" {
		wantFindings, wantQueued, wantAcknowledged = nil, false, false
	}
	return result.State == wantState && result.Diagnostic == wantDiagnostic && result.ReviewState == review.State && result.ReviewDiagnostic == current.ReviewDiagnostic && result.ReviewMode == review.Mode && result.ReviewTarget == review.Target && result.ReviewBase == review.Base && result.ReviewHead == review.Head && result.ReviewSnapshot == review.Snapshot && result.ReviewSession == review.Session && slices.Equal(result.ReviewFindings, wantFindings) && result.ReviewHandoffQueued == wantQueued && result.ReviewHandoffAck == wantAcknowledged
}

func validRuntimeEffectAction(action agentruntime.EffectAction) bool {
	return slices.Contains([]agentruntime.EffectAction{
		agentruntime.EffectPrepare,
		agentruntime.EffectStart,
		agentruntime.EffectMonitor,
		agentruntime.EffectStop,
		agentruntime.EffectReview,
		agentruntime.EffectHandoff,
		agentruntime.EffectCleanup,
	}, action)
}

func validRuntimeEffectInput(action agentruntime.EffectAction, reason string) bool {
	if !validRuntimeEffectAction(action) || len(reason) > 4096 || strings.ContainsRune(reason, 0) {
		return false
	}
	return action == agentruntime.EffectStop && strings.TrimSpace(reason) != "" || action != agentruntime.EffectStop && reason == ""
}

func runtimeEffectAppliesManifest(action agentruntime.EffectAction) bool {
	return action != agentruntime.EffectCleanup
}

func hasAttemptEffect(state runtimeOwnerState, repository string, issue, attempt int) bool {
	for _, effect := range state.Effects {
		if effect.Repository == repository && effect.Issue == issue && effect.Attempt == attempt {
			return true
		}
	}
	return false
}

func pruneCompletedAttemptEffects(state *runtimeOwnerState, repository string, issue, attempt int) {
	tombstoneEffect := ""
	if tombstone, ok := state.Tombstones[ownerAttemptKey(repository, issue, attempt)]; ok {
		tombstoneEffect = tombstone.EffectID
	}
	for id, effect := range state.Effects {
		if id != tombstoneEffect && !effectReferencedByReceipt(*state, id) && effect.Repository == repository && effect.Issue == issue && effect.Attempt == attempt && effect.State == "completed" {
			delete(state.Effects, id)
		}
	}
}

func pendingReviewerDeathUnproved(state runtimeOwnerState, effect runtimeEffectIntent) bool {
	if effect.State != "pending" || effect.Reconciliation == nil || effect.Reconciliation.Action != reconciliationReviewer || effect.Reconciliation.Reviewer == nil || effect.Reconciliation.Reviewer.Phase != "run-observe" {
		return false
	}
	reviewer := effect.Reconciliation.Reviewer
	proof, ok := state.ReviewerProofs[reviewerProofKey(effect.Repository, effect.Issue, effect.Attempt, reviewer.Mode, reviewer.Target)]
	return !ok || proof.EffectID != effect.ID || !proof.DeadProved
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
	if issueHasUnprovedReviewer(*state, command.Repository, command.Issue) {
		return errStateConflict
	}
	for _, effect := range state.Effects {
		if effect.Repository == command.Repository && effect.Issue == command.Issue && !effectAuthorizedByTombstone(*state, effect) && pendingReviewerDeathUnproved(*state, effect) {
			return errStateConflict
		}
	}
	state.IssueGenerations[key] = generation + 1
	if current, exists := state.MachineStatuses[key]; exists {
		if err := applyAdmitMachineStatus(state, admitMachineStatusCommand{Repository: command.Repository, Issue: command.Issue, Attempt: current.Attempt, ExpectedIssueGeneration: generation + 1, ExpectedAttemptGeneration: state.AttemptGenerations[ownerAttemptKey(command.Repository, command.Issue, current.Attempt)], ExpectedStatusSequence: current.Sequence, Source: "destructive", SourceID: fmt.Sprintf("issue:%d", generation+1), Status: "clear", Reason: "issue generation invalidated"}); err != nil {
			return err
		}
	}
	delete(state.Observations, key)
	deleteIssueRecoveries(state, command.Repository, command.Issue)
	for id, effect := range state.Effects {
		if effect.Repository == command.Repository && effect.Issue == command.Issue && !effectAuthorizedByTombstone(*state, effect) && !invalidatedExternalEffect(*state, effect) {
			delete(state.Effects, id)
		}
	}
	return nil
}

func issueHasUnprovedReviewer(state runtimeOwnerState, repository string, issue int) bool {
	if state.LegacyReviewerQuarantines[ownerIssueKey(repository, issue)] != "" {
		return true
	}
	for _, proof := range state.ReviewerProofs {
		if proof.Repository == repository && proof.Issue == issue && !proof.NeverRan {
			return true
		}
	}
	return false
}

// Older group-only death certificates and completed cleanup records cannot
// establish descendant absence. Preserve old business receipts and quarantine
// only the affected issue; never synthesize a physical-completion proof.
func migrateLegacyReviewerSafety(state *runtimeOwnerState) {
	if state.LegacyReviewerQuarantines == nil {
		state.LegacyReviewerQuarantines = map[string]string{}
	}
	if state.ReviewerProofs == nil {
		state.ReviewerProofs = map[string]reviewerProcessProof{}
	}
	for key, proof := range state.ReviewerProofs {
		if proof.NeverRan {
			continue
		}
		proof.DeadProved = false
		proof.LegacyUnverified = true
		state.ReviewerProofs[key] = proof
	}
	for _, effect := range state.Effects {
		if !effect.ReviewerLaunched {
			continue
		}
		issueKey := ownerIssueKey(effect.Repository, effect.Issue)
		if effect.Reconciliation == nil || effect.Reconciliation.Reviewer == nil || effect.ReviewerGroupPID < 2 {
			state.LegacyReviewerQuarantines[issueKey] = "legacy reviewer identity is incomplete; physical cleanup cannot be certified"
			continue
		}
		reviewer := effect.Reconciliation.Reviewer
		key := reviewerProofKey(effect.Repository, effect.Issue, effect.Attempt, reviewer.Mode, reviewer.Target)
		if existing, ok := state.ReviewerProofs[key]; ok {
			if existing.EffectID != effect.ID {
				state.LegacyReviewerQuarantines[issueKey] = "legacy reviewer identity conflicts; physical cleanup cannot be certified"
			}
			continue
		}
		state.ReviewerProofs[key] = reviewerProcessProof{Repository: effect.Repository, Issue: effect.Issue, Attempt: effect.Attempt, Mode: reviewer.Mode, Target: reviewer.Target, EffectID: effect.ID, IssueGeneration: effect.IssueGeneration, AttemptGeneration: effect.AttemptGeneration, GroupPID: effect.ReviewerGroupPID, LegacyUnverified: true}
	}
	for _, record := range state.Attempts {
		manifest := record.Manifest
		if manifest.ReviewState == "" {
			continue
		}
		found := false
		for _, proof := range state.ReviewerProofs {
			if proof.Repository == manifest.Repository && proof.Issue == manifest.Issue && proof.Attempt == manifest.Attempt && !proof.NeverRan {
				found = true
				break
			}
		}
		if !found {
			state.LegacyReviewerQuarantines[ownerIssueKey(manifest.Repository, manifest.Issue)] = "legacy reviewer history is incomplete; physical cleanup cannot be certified"
		}
	}
	for _, tombstone := range state.Tombstones {
		bound := false
		for _, proof := range state.ReviewerProofs {
			if proof.Repository == tombstone.Repository && proof.Issue == tombstone.Issue && proof.Attempt == tombstone.Attempt && !proof.NeverRan {
				bound = true
			}
		}
		if bound {
			continue
		}
		if !bound {
			state.LegacyReviewerQuarantines[ownerIssueKey(tombstone.Repository, tombstone.Issue)] = "legacy reviewer absence unknown; physical cleanup cannot be certified"
		}
	}
}

func attemptHasUnprovedReviewer(state runtimeOwnerState, repository string, issue, attempt int) bool {
	for _, proof := range state.ReviewerProofs {
		if proof.Repository == repository && proof.Issue == issue && proof.Attempt == attempt && !proof.NeverRan {
			return true
		}
	}
	return false
}

func finishRuntimeOwnerTransition(attemptRoot, stateRoot string, candidate runtimeOwnerState, effect *runtimeEffectIntent, operatorRequestIDs ...string) (runtimeOwnerState, *runtimeEffectIntent, error) {
	if candidate.Revision == ^uint64(0) {
		return runtimeOwnerState{}, nil, errors.New("runtime revision overflow")
	}
	candidate.Revision++
	if effect != nil && effect.ID == "" {
		effect.IntentRevision = candidate.Revision
		effect.ID = runtimeEffectID(*effect)
		candidate.Effects[effect.ID] = *effect
		attemptKey := ownerAttemptKey(effect.Repository, effect.Issue, effect.Attempt)
		if tombstone, ok := candidate.Tombstones[attemptKey]; ok && tombstone.Revision == 0 {
			tombstone.EffectID = effect.ID
			candidate.Tombstones[attemptKey] = tombstone
		}
		if effect.Action == string(agentruntime.EffectStop) {
			if record, ok := candidate.Attempts[attemptKey]; ok && record.Generation == effect.AttemptGeneration {
				record.StopEffectID = effect.ID
				candidate.Attempts[attemptKey] = record
			}
		}
	}
	for _, requestID := range operatorRequestIDs {
		bindOperatorReceipt(&candidate, requestID, effect, candidate.Revision)
	}
	for key, tombstone := range candidate.Tombstones {
		if tombstone.Revision == 0 {
			tombstone.Revision = candidate.Revision
			candidate.Tombstones[key] = tombstone
		}
	}
	revokeInvalidPlanReviewers(&candidate, false)
	if err := validateRuntimeOwnerState(candidate, attemptRoot, stateRoot, true); err != nil {
		return runtimeOwnerState{}, nil, err
	}
	return candidate, cloneEffect(effect), nil
}

func applyUpsertAttempt(attemptRoot, stateRoot string, state *runtimeOwnerState, command upsertAttemptCommand) error {
	return applyUpsertAttemptAllowingReviewer(attemptRoot, stateRoot, state, command, "")
}

func applyUpsertAttemptAllowingReviewer(attemptRoot, stateRoot string, state *runtimeOwnerState, command upsertAttemptCommand, supersededReviewerID string) error {
	manifest := cloneManifest(command.Manifest)
	if err := validateOwnerManifest(state.Repository, attemptRoot, stateRoot, manifest); err != nil {
		return err
	}
	issueKey, attemptKey := ownerIssueKey(manifest.Repository, manifest.Issue), ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)
	issueGeneration := state.IssueGenerations[issueKey]
	if issueGeneration != command.ExpectedIssueGeneration {
		return errStaleStateResult
	}
	if _, tombstoned := state.Tombstones[attemptKey]; tombstoned {
		return errAttemptTombstoned
	}
	if state.LegacyReviewerQuarantines[issueKey] != "" {
		return errStateConflict
	}
	for _, proof := range state.ReviewerProofs {
		if proof.Repository == manifest.Repository && proof.Issue == manifest.Issue && proof.Attempt != manifest.Attempt && !proof.NeverRan {
			return errStateConflict
		}
	}
	generation := state.AttemptGenerations[attemptKey]
	if generation != command.ExpectedAttemptGeneration {
		return errStaleStateResult
	}
	if state.Attempts[attemptKey].StopEffectID != "" {
		return errStateConflict
	}
	if generation == 0 {
		if issueHasUnprovedReviewer(*state, manifest.Repository, manifest.Issue) {
			return errStateConflict
		}
		if issueGeneration == ^uint64(0) {
			return errors.New("issue generation overflow")
		}
		issueGeneration++
		state.IssueGenerations[issueKey] = issueGeneration
		generation = 1
		state.AttemptGenerations[attemptKey] = generation
		if current, exists := state.MachineStatuses[issueKey]; exists {
			if err := applyAdmitMachineStatus(state, admitMachineStatusCommand{Repository: manifest.Repository, Issue: manifest.Issue, Attempt: manifest.Attempt, ExpectedIssueGeneration: issueGeneration, ExpectedAttemptGeneration: generation, ExpectedStatusSequence: current.Sequence, Source: "destructive", SourceID: fmt.Sprintf("attempt:%d", generation), Status: "clear", Reason: "attempt superseded"}); err != nil {
				return err
			}
		}
		if err := pruneSupersededIssueEffects(state, manifest.Repository, manifest.Issue, issueGeneration); err != nil {
			return err
		}
		delete(state.Observations, issueKey)
		deleteIssueRecoveries(state, manifest.Repository, manifest.Issue)
	} else {
		if generation == ^uint64(0) {
			return errors.New("attempt generation overflow")
		}
		for _, effect := range state.Effects {
			if effect.Repository == manifest.Repository && effect.Issue == manifest.Issue && effect.Attempt == manifest.Attempt && effect.ID != supersededReviewerID && !effectReferencedByReceipt(*state, effect.ID) && pendingReviewerDeathUnproved(*state, effect) {
				return errStateConflict
			}
		}
		generation++
		deleteAttemptEffects(state, manifest.Repository, manifest.Issue, manifest.Attempt)
		delete(state.Recoveries, attemptKey)
	}
	state.AttemptGenerations[attemptKey] = generation
	if err := deleteAttemptObservation(state, manifest.Repository, manifest.Issue, manifest.Attempt); err != nil {
		return err
	}
	previous := state.Attempts[attemptKey]
	state.Attempts[attemptKey] = runtimeAttemptRecord{Generation: generation, ObservationEpoch: previous.ObservationEpoch, LastCycleID: previous.LastCycleID, Manifest: manifest}
	return nil
}

func pendingHandoffCandidate(state runtimeOwnerState, repository string, issue, attempt int) (*handoffCandidateInvalidation, error) {
	var candidate *handoffCandidateInvalidation
	for _, effect := range state.Effects {
		if effect.State != "pending" || effect.Action != string(reconciliationHandoffDeliver) || effect.Repository != repository || effect.Issue != issue || effect.Attempt != attempt || effect.Reconciliation == nil || effect.Reconciliation.Manifest == nil || effect.Reconciliation.Handoff == nil {
			continue
		}
		manifest := *effect.Reconciliation.Manifest
		if manifest.Version != agentruntime.ManifestVersion2 {
			continue
		}
		if candidate != nil || !agentruntime.ValidLaunchToken(effect.Reconciliation.Handoff.CandidateLaunchToken) {
			return nil, errStateConflict
		}
		candidate = &handoffCandidateInvalidation{EffectID: effect.ID, Token: effect.Reconciliation.Handoff.CandidateLaunchToken, Key: effect.Reconciliation.Handoff.Key, Manifest: cloneManifest(manifest)}
	}
	return candidate, nil
}

func pendingStartCandidate(state runtimeOwnerState, manifest agentruntime.Manifest) (*startCandidateInvalidation, error) {
	var candidate *startCandidateInvalidation
	for _, effect := range state.Effects {
		if effect.State != "pending" || effect.Action != string(agentruntime.EffectStart) || effect.Repository != manifest.Repository || effect.Issue != manifest.Issue || effect.Attempt != manifest.Attempt {
			continue
		}
		if candidate != nil || manifest.Version != agentruntime.ManifestVersion2 {
			return nil, errStateConflict
		}
		gates := slices.Clone(effect.StartCandidates)
		if len(gates) == 0 {
			gates = []startGateCandidate{{Nonce: effect.ID, MayRun: true}} // Legacy intent has no pre-release proof.
		}
		candidate = &startCandidateInvalidation{EffectID: effect.ID, Manifest: cloneManifest(manifest), Candidates: gates}
	}
	return candidate, nil
}

func controlGeneration(state runtimeOwnerState, issueKey string) uint64 {
	if generation := state.ControlGenerations[issueKey]; generation > 0 {
		return generation
	}
	return 1
}

func invalidateControlSnapshot(state *runtimeOwnerState, issueKey string) {
	generation := controlGeneration(*state, issueKey) + 1
	if state.ControlGenerations == nil {
		state.ControlGenerations = map[string]uint64{}
	}
	if state.ControlRepairs == nil {
		state.ControlRepairs = map[string]controlSnapshotRepair{}
	}
	state.ControlGenerations[issueKey] = generation
	body := state.ControlRepairs[issueKey].Body
	for id, effect := range state.Effects {
		request := effect.Reconciliation
		if effect.State != "pending" || request == nil || ownerIssueKey(effect.Repository, effect.Issue) != issueKey || request.Action != reconciliationGitHubIssueUpdate || request.GitHubIssueUpdate == nil || request.GitHubIssueUpdate.Kind != githubIssueControlSnapshot {
			continue
		}
		if request.GitHubIssueUpdate.ControlSnapshotBody != "" {
			body = request.GitHubIssueUpdate.ControlSnapshotBody
		}
		if !effect.Dispatched {
			delete(state.Effects, id)
			continue
		}
		effect.State = "invalidated"
		state.Effects[id] = effect
	}
	if body == "" {
		return
	}
	snapshot, err := internalgithub.ParseSnapshotComment(body, 1, 1)
	if err != nil {
		return
	}
	snapshot.OwnerGeneration = generation
	state.ControlRepairs[issueKey] = controlSnapshotRepair{Generation: generation, Body: internalgithub.SnapshotComment(snapshot)}
}

func applyInvalidateAttempt(attemptRoot, stateRoot string, state *runtimeOwnerState, command invalidateAttemptCommand) (*runtimeEffectIntent, error) {
	if command.Repository != state.Repository || command.Issue < 1 || command.Attempt < 1 || !validTombstoneAction(command.Action) || !validCleanupPhase(command.CleanupPhase) {
		return nil, errStateConflict
	}
	issueKey, attemptKey := ownerIssueKey(command.Repository, command.Issue), ownerAttemptKey(command.Repository, command.Issue, command.Attempt)
	if state.IssueGenerations[issueKey] != command.ExpectedIssueGeneration {
		return nil, errStaleStateResult
	}
	if existing, ok := state.Tombstones[attemptKey]; ok {
		if existing.InvalidatedGeneration != command.ExpectedAttemptGeneration || existing.Action != command.Action || existing.CleanupPhase != command.CleanupPhase || existing.PublishedHead != command.PublishedHead || existing.Diagnostic != command.Diagnostic || !sameOptionalAttemptIdentity(existing.Manifest, command.Manifest) || !reflect.DeepEqual(existing.CleanupPolicy, command.CleanupPolicy) {
			return nil, errStateConflict
		}
		if command.EffectAction == "" && existing.EffectID == "" {
			return nil, nil
		}
		effect, ok := state.Effects[existing.EffectID]
		if !ok || effect.Action != command.EffectAction || effect.RequestDigest != command.EffectRequestDigest || effect.Repository != command.Repository || effect.Issue != command.Issue || effect.Attempt != command.Attempt {
			return nil, errStateConflict
		}
		return cloneEffect(&effect), nil
	}
	if state.AttemptGenerations[attemptKey] != command.ExpectedAttemptGeneration {
		return nil, errStaleStateResult
	}
	if command.Manifest != nil {
		manifest := cloneManifest(*command.Manifest)
		if err := validateOwnerManifest(state.Repository, attemptRoot, stateRoot, manifest); err != nil || manifest.Issue != command.Issue || manifest.Attempt != command.Attempt {
			return nil, errStateConflict
		}
		command.Manifest = &manifest
	}
	invalidateControlSnapshot(state, issueKey)
	invalidatedHandoff, err := pendingHandoffCandidate(*state, command.Repository, command.Issue, command.Attempt)
	if err != nil {
		return nil, err
	}
	var invalidatedStart *startCandidateInvalidation
	if command.Manifest != nil {
		invalidatedStart, err = pendingStartCandidate(*state, *command.Manifest)
		if err != nil {
			return nil, err
		}
		if invalidatedStart != nil && agentruntime.WorkerConfinementBound(invalidatedStart.Manifest, command.ExpectedAttemptGeneration, activeWorkerProfileDigest(*state)) {
			// The generation change revokes all owner authority held by this
			// rootless candidate. Its physically escaped descendants have no
			// network, credentials, state, socket, or sibling-workspace access.
			invalidatedStart = nil
		}
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
	if err := deleteAttemptObservation(state, command.Repository, command.Issue, command.Attempt); err != nil {
		return nil, err
	}
	delete(state.Recoveries, attemptKey)
	invalidateAttemptEffects(state, command.Repository, command.Issue, command.Attempt)
	state.Tombstones[attemptKey] = runtimeTombstone{
		Repository: command.Repository, Issue: command.Issue, Attempt: command.Attempt, Action: command.Action,
		InvalidatedGeneration: invalidated, Generation: generation, CleanupPhase: command.CleanupPhase,
		PublishedHead: command.PublishedHead, Manifest: command.Manifest, CleanupPolicy: cloneCleanupPolicy(command.CleanupPolicy), Diagnostic: command.Diagnostic, InvalidatedHandoff: invalidatedHandoff, InvalidatedStart: invalidatedStart, ExternalOutcomes: map[string]invalidatedExternalOutcome{},
	}
	if err := applyAdmitMachineStatus(state, admitMachineStatusCommand{
		Repository: command.Repository, Issue: command.Issue, Attempt: command.Attempt,
		ExpectedIssueGeneration: command.ExpectedIssueGeneration, ExpectedAttemptGeneration: generation,
		ExpectedStatusSequence: state.MachineStatuses[issueKey].Sequence,
		Source:                 "destructive", SourceID: fmt.Sprintf("%s:%d", command.Action, generation), Status: "clear", Reason: "attempt invalidated",
	}); err != nil {
		return nil, err
	}
	if command.EffectAction == "" {
		return nil, nil
	}
	if command.EffectAction != string(agentruntime.EffectCleanup) || !agentruntime.ValidEffectRequestDigest(command.EffectRequestDigest) {
		return nil, errStateConflict
	}
	return &runtimeEffectIntent{Action: command.EffectAction, Repository: command.Repository, Issue: command.Issue, Attempt: command.Attempt, IssueGeneration: command.ExpectedIssueGeneration, AttemptGeneration: generation, IntentEpoch: state.Epoch, State: "pending", RequestDigest: command.EffectRequestDigest}, nil
}

func applySelectWorkerSeal(stateRoot string, state *runtimeOwnerState, command selectWorkerSealCommand) error {
	key := ownerAttemptKey(command.Repository, command.Issue, command.Attempt)
	record, ok := state.Attempts[key]
	if !ok || command.Repository != state.Repository || command.ExpectedGeneration == 0 || record.Generation != command.ExpectedGeneration || state.AttemptGenerations[key] != command.ExpectedGeneration {
		return errStaleStateResult
	}
	if _, invalidated := state.Tombstones[key]; invalidated || record.Manifest.State != "completed" || !validWorkerSealSelection(stateRoot, record.Manifest, command.ExpectedGeneration, command.Selection) {
		return errStateConflict
	}
	if record.WorkerSeal != nil {
		if *record.WorkerSeal == command.Selection {
			return nil
		}
		return errStateConflict
	}
	selection := command.Selection
	record.WorkerSeal = &selection
	state.Attempts[key] = record
	return nil
}

func applyAdmitMachineStatus(state *runtimeOwnerState, command admitMachineStatusCommand) error {
	command.Reason = strings.TrimSpace(command.Reason)
	issueKey := ownerIssueKey(command.Repository, command.Issue)
	if command.Repository != state.Repository || command.Issue < 1 || command.Attempt < 1 ||
		state.IssueGenerations[issueKey] != command.ExpectedIssueGeneration || command.ExpectedIssueGeneration == 0 ||
		!slices.Contains([]string{"worker", "dependency", "orchestrator", "destructive"}, command.Source) ||
		!boundedText(command.SourceID, 128, true) || !slices.Contains([]string{"needs-attention", "clear"}, command.Status) ||
		!boundedText(command.Reason, 1024, true) {
		return errStaleStateResult
	}
	attemptKey := ownerAttemptKey(command.Repository, command.Issue, command.Attempt)
	if command.Source == "dependency" {
		observation, ok := state.Observations[issueKey]
		proposal := reconciliationIssueUpdateProposal{Repository: command.Repository, Issue: command.Issue, Kind: githubIssueDependencyClear, AttributionAttempt: command.Attempt, Dependency: command.Dependency, PullRequest: command.PullRequest}
		current := state.MachineStatuses[issueKey]
		if !ok || !observation.Present || observation.Generation != command.ExpectedObservationGeneration || observation.OwnerGeneration != command.ExpectedIssueGeneration || !slices.Contains(observation.IssueUpdates, proposal) ||
			command.ExpectedAttemptGeneration == 0 || state.AttemptGenerations[attemptKey] != command.ExpectedAttemptGeneration || current.Repository != "" && current.Source != "dependency" {
			return errStaleStateResult
		}
	} else if command.Source != "destructive" {
		record, ok := state.Attempts[attemptKey]
		if !ok || record.Generation != command.ExpectedAttemptGeneration || state.AttemptGenerations[attemptKey] != command.ExpectedAttemptGeneration {
			return errStaleStateResult
		}
	} else if state.AttemptGenerations[attemptKey] != command.ExpectedAttemptGeneration {
		return errStaleStateResult
	}
	current := state.MachineStatuses[issueKey]
	if current.Repository != "" && current.Source == command.Source && current.SourceID == command.SourceID && current.Attempt == command.Attempt && current.AttemptGeneration == command.ExpectedAttemptGeneration && current.Status == command.Status && current.Reason == command.Reason {
		return nil
	}
	if current.Sequence != command.ExpectedStatusSequence {
		return errStaleStateResult
	}
	if current.Sequence == ^uint64(0) {
		return errors.New("machine status sequence overflow")
	}
	sequence := current.Sequence + 1
	if sequence == 0 {
		sequence = 1
	}
	for id, effect := range state.Effects {
		request := effect.Reconciliation
		if effect.Repository != command.Repository || effect.Issue != command.Issue || effect.State != "pending" || request == nil || request.GitHubIssueUpdate == nil || request.GitHubIssueUpdate.Kind != githubIssueMachineStatus {
			continue
		}
		if effect.Dispatched {
			effect.State, effect.Diagnostic = "invalidated", ""
			state.Effects[id] = effect
		} else {
			delete(state.Effects, id)
		}
	}
	state.MachineStatuses[issueKey] = machineStatusRecord{
		Repository: command.Repository, Issue: command.Issue, Attempt: command.Attempt,
		IssueGeneration: command.ExpectedIssueGeneration, AttemptGeneration: command.ExpectedAttemptGeneration,
		Sequence: sequence, AppliedSequence: current.AppliedSequence, Source: command.Source,
		SourceID: command.SourceID, SourceSequence: sequence, Status: command.Status, Reason: strings.TrimSpace(command.Reason),
	}
	return nil
}

func validWorkerSealSelection(stateRoot string, manifest agentruntime.Manifest, generation uint64, selection workerSealSelection) bool {
	expectedRoot := workerSealPath(stateRoot, generation, manifest, selection.HeadSHA)
	return generation != 0 && selection.Generation == generation &&
		preflightObjectID.MatchString(selection.HeadSHA) && selection.HeadSHA != manifest.BaseSHA &&
		validDigest(selection.BundleSHA256) && validDigest(selection.ProfileDigest) &&
		selection.ProfileDigest == manifest.WorkerProfileDigest &&
		selection.Root == expectedRoot &&
		selection.Result.Type == "agent-symphony-result-v1" &&
		boundedText(selection.Result.Validation, maxReconciliationStringBytes, true) &&
		boundedText(selection.Result.Documentation, maxReconciliationStringBytes, true) &&
		boundedText(selection.Result.Decisions, maxReconciliationStringBytes, false)
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
	if (effect.Action == string(agentruntime.EffectCleanup) || effect.Action == string(agentruntime.EffectStop)) && (attemptHasUnprovedReviewer(*state, effect.Repository, effect.Issue, effect.Attempt) || state.LegacyReviewerQuarantines[ownerIssueKey(effect.Repository, effect.Issue)] != "") {
		return errStateConflict
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
	if !validControlRequest(receipt.Request, state.Repository) || receipt.State != "pending" && receipt.State != "completed" || receipt.State == "pending" && receipt.Result != nil || receipt.State == "completed" && (receipt.Result == nil || !validRecordedControlResult(*receipt.Result, receipt.Request)) || !validOperatorReceiptBinding(receipt) {
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

func applyRecordOperatorDiagnostic(state *runtimeOwnerState, command recordOperatorDiagnosticCommand) error {
	if command.RequestID == "" || command.Phase != operatorPhaseTerminalAwait && command.Phase != operatorPhaseRetryAwait && command.Phase != operatorPhaseHandoffCleanup && command.Phase != operatorPhaseStartCleanup || !boundedText(command.Diagnostic, maxReconciliationStringBytes, true) {
		return errStateConflict
	}
	for index := range state.ControlReceipts {
		receipt := &state.ControlReceipts[index]
		if receipt.Request.RequestID != command.RequestID {
			continue
		}
		if receipt.State != "pending" || receipt.Phase != command.Phase || receipt.EffectID != command.EffectID {
			return errStaleStateResult
		}
		receipt.Diagnostic = command.Diagnostic
		return nil
	}
	return errStaleStateResult
}

func applyCompleteHandoffCompensation(state *runtimeOwnerState, command completeHandoffCompensationCommand) error {
	if command.RequestID == "" || command.Proof.EffectID == "" {
		return errStateConflict
	}
	var tombstoneKey string
	for _, receipt := range state.ControlReceipts {
		if receipt.Request.RequestID != command.RequestID {
			continue
		}
		if receipt.Request.Action != "dismiss" {
			return errStateConflict
		}
		tombstoneKey = ownerAttemptKey(receipt.Request.Repository, receipt.Request.Issue, receipt.Request.Attempt)
		break
	}
	if tombstoneKey == "" {
		return errStaleStateResult
	}
	tombstone, ok := state.Tombstones[tombstoneKey]
	if !ok || tombstone.Action != "dismissed" || tombstone.InvalidatedHandoff == nil || tombstone.InvalidatedHandoff.EffectID != command.Proof.EffectID || tombstone.InvalidatedHandoff.Token != command.Proof.Token || !command.Proof.PreserveOld || command.Proof.OldSessionID == "" || command.Proof.OldPaneID == "" || !slices.Contains([]string{"absent", "marked-old", "killed"}, command.Proof.Disposition) {
		return errStaleStateResult
	}
	if tombstone.HandoffCompensated {
		return nil
	}
	completed := false
	for index := range state.ControlReceipts {
		receipt := &state.ControlReceipts[index]
		if receipt.State == "pending" && receipt.Phase == operatorPhaseHandoffCleanup && ownerAttemptKey(receipt.Request.Repository, receipt.Request.Issue, receipt.Request.Attempt) == tombstoneKey {
			receipt.State, receipt.Phase, receipt.Diagnostic = "completed", operatorPhaseCompleted, ""
			receipt.Result = successfulOperatorResult(receipt.Request, state.Revision+1)
			completed = true
		}
	}
	if !completed {
		return errStateConflict
	}
	tombstone.HandoffCompensated = true
	state.Tombstones[tombstoneKey] = tombstone
	return nil
}

func loadOrMigrateRuntimeOwnerState(stateRoot, legacyRecoveryPath, repository string) (runtimeOwnerState, bool, error) {
	state, err := readRuntimeOwnerState(stateRoot, repository)
	if err == nil {
		return state, false, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return runtimeOwnerState{}, false, err
	}
	state, err = migrateLegacyRuntimeState(stateRoot, legacyRecoveryPath, repository)
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
	if state.ControlGenerations == nil {
		state.ControlGenerations = map[string]uint64{}
	}
	if state.ControlRepairs == nil {
		state.ControlRepairs = map[string]controlSnapshotRepair{}
	}
	for key, tombstone := range state.Tombstones {
		if tombstone.ExternalOutcomes == nil {
			tombstone.ExternalOutcomes = map[string]invalidatedExternalOutcome{}
			state.Tombstones[key] = tombstone
		}
	}
	migrateLegacyMachineStatuses(&state)
	if err := validateRuntimeOwnerState(state, runtimeOwnerAttemptRoot(stateRoot), stateRoot, true); err != nil || state.Repository != repository {
		if err == nil {
			err = fmt.Errorf("runtime state is bound to project %s, not %s", state.Repository, repository)
		}
		return runtimeOwnerState{}, err
	}
	return state, nil
}

// Ledgers written before machine status joined the issue-scoped owner domain
// stored worker status effects against an attempt. Rebuild one current intent
// from each issue's newest manifest and let normal reconciliation publish the
// canonical owner marker.
func migrateLegacyMachineStatuses(state *runtimeOwnerState) {
	if state.MachineStatuses != nil {
		for key, status := range state.MachineStatuses {
			if status.SourceID == "" {
				status.SourceID = fmt.Sprintf("legacy:%s:%d", status.Source, status.SourceSequence)
			}
			status.SourceSequence = status.Sequence
			state.MachineStatuses[key] = status
		}
		return
	}
	state.MachineStatuses = map[string]machineStatusRecord{}
	for key, record := range state.Attempts {
		manifest := record.Manifest
		if manifest.WorkerStatusSeq == 0 || !slices.Contains([]string{"needs-attention", "clear"}, manifest.WorkerStatus) || strings.TrimSpace(manifest.WorkerStatusReason) == "" {
			continue
		}
		issueKey := ownerIssueKey(manifest.Repository, manifest.Issue)
		current := state.MachineStatuses[issueKey]
		if current.Repository != "" && current.Attempt >= manifest.Attempt {
			continue
		}
		state.MachineStatuses[issueKey] = machineStatusRecord{Repository: manifest.Repository, Issue: manifest.Issue, Attempt: manifest.Attempt, IssueGeneration: state.IssueGenerations[issueKey], AttemptGeneration: state.AttemptGenerations[key], Sequence: 1, Source: "worker", SourceID: fmt.Sprintf("legacy:worker:%d", manifest.WorkerStatusSeq), SourceSequence: 1, Status: manifest.WorkerStatus, Reason: strings.TrimSpace(manifest.WorkerStatusReason)}
	}
	for id, effect := range state.Effects {
		if effect.Reconciliation != nil && effect.Reconciliation.GitHubIssueUpdate != nil && effect.Reconciliation.GitHubIssueUpdate.Kind == githubIssueMachineStatus && effect.Attempt > 0 {
			issueKey := ownerIssueKey(effect.Repository, effect.Issue)
			if _, current := state.MachineStatuses[issueKey]; !current {
				attemptGeneration := state.AttemptGenerations[ownerAttemptKey(effect.Repository, effect.Issue, effect.Attempt)]
				if state.IssueGenerations[issueKey] > 0 && attemptGeneration > 0 {
					state.MachineStatuses[issueKey] = machineStatusRecord{Repository: effect.Repository, Issue: effect.Issue, Attempt: effect.Attempt, IssueGeneration: state.IssueGenerations[issueKey], AttemptGeneration: attemptGeneration, Sequence: 1, Source: "destructive", SourceID: fmt.Sprintf("legacy:destructive:%d", attemptGeneration), SourceSequence: 1, Status: "clear", Reason: "legacy status intent invalidated"}
				}
			}
			delete(state.Effects, id)
		}
	}
}

func migrateLegacyRuntimeState(stateRoot, legacyRecoveryPath, repository string) (runtimeOwnerState, error) {
	identity, err := readDeploymentIdentity(stateRoot)
	if err != nil {
		return runtimeOwnerState{}, fmt.Errorf("read deployment identity: %w", err)
	}
	if identity.Repository != repository {
		return runtimeOwnerState{}, fmt.Errorf("runtime state is bound to project %s, not %s", identity.Repository, repository)
	}
	state := newRuntimeOwnerState(repository)
	state.ReviewerSafetyMigrated = false // Pre-ledger cleanup had no descendant proof.
	server := dashboardServer{stateRoot: stateRoot, repository: repository}
	manifests, err := (&agentruntime.Runtime{Root: productionAttemptRoot(stateRoot), StateRoot: stateRoot}).Discover()
	if err != nil {
		return runtimeOwnerState{}, fmt.Errorf("migrate attempt manifests: %w", err)
	}
	for _, manifest := range manifests {
		if manifest.ReviewState == "preparing" || manifest.ReviewState == "running" {
			return runtimeOwnerState{}, fmt.Errorf("legacy attempt %d/%d has an active reviewer", manifest.Issue, manifest.Attempt)
		}
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
	}
	receipts, err := server.readControlReceipts()
	if err != nil {
		return runtimeOwnerState{}, fmt.Errorf("migrate control receipts: %w", err)
	}
	for _, receipt := range receipts.Receipts {
		state.ControlReceipts = append(state.ControlReceipts, cloneControlReceipt(receipt))
	}
	if err := bindMigratedRemovalReceipts(&state); err != nil {
		return runtimeOwnerState{}, err
	}
	recoveries, err := readLegacyPRRecoveries(legacyRecoveryPath)
	if err != nil {
		return runtimeOwnerState{}, fmt.Errorf("migrate pull-request recovery state: %w", err)
	}
	seenPRs := map[int]bool{}
	for _, recovery := range recoveries {
		key := ownerAttemptKey(recovery.Repository, recovery.Issue, recovery.Attempt)
		if !validPRState(recovery) || recovery.Repository != repository || seenPRs[recovery.Number] {
			return runtimeOwnerState{}, errors.New("legacy pull-request recovery identity is invalid")
		}
		seenPRs[recovery.Number] = true
		if _, tombstoned := state.Tombstones[key]; tombstoned {
			continue
		}
		if _, duplicate := state.Recoveries[key]; duplicate {
			return runtimeOwnerState{}, errors.New("legacy pull-request recovery attempt is duplicated")
		}
		if _, current := state.Attempts[key]; !current {
			return runtimeOwnerState{}, errors.New("legacy pull-request recovery has no current attempt")
		}
		state.Recoveries[key] = runtimePRRecovery{IssueGeneration: state.IssueGenerations[ownerIssueKey(repository, recovery.Issue)], AttemptGeneration: state.AttemptGenerations[key], State: recovery}
	}
	if err := validateRuntimeOwnerState(state, runtimeOwnerAttemptRoot(stateRoot), stateRoot, false); err != nil {
		return runtimeOwnerState{}, err
	}
	return state, nil
}

func readLegacyPRRecoveries(path string) ([]internalgithub.PRState, error) {
	if path == "" {
		return nil, errors.New("legacy pull-request recovery path is required")
	}
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || !ownedByCurrentUser(info) || info.Size() > maxRuntimeOwnerState {
		return nil, errors.New("legacy pull-request recovery file is unsafe")
	}
	var states []internalgithub.PRState
	decoder := json.NewDecoder(io.LimitReader(file, maxRuntimeOwnerState+1))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&states) != nil || decoder.Decode(&struct{}{}) != io.EOF || len(states) > maxPRRecoveryEntries {
		return nil, errors.New("legacy pull-request recovery file is invalid")
	}
	return states, nil
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
	if exists && action == "removed" {
		invalidated, generation = existing.InvalidatedGeneration, existing.Generation
	}
	if action == "removed" && manifest == nil && phase != "completed" {
		return errors.New("legacy pending permanent removal is missing its manifest identity")
	}
	if manifest == nil {
		if record, ok := state.Attempts[attemptKey]; ok {
			copy := cloneManifest(record.Manifest)
			manifest = &copy
		}
	} else if existing.Manifest != nil && !sameAttemptIdentity(*existing.Manifest, *manifest) {
		return fmt.Errorf("legacy attempt %s has conflicting resource identity", attemptKey)
	}
	if existing.EffectID != "" {
		delete(state.Effects, existing.EffectID)
	}
	delete(state.Attempts, attemptKey)
	state.AttemptGenerations[attemptKey] = generation
	tombstone := runtimeTombstone{Repository: repository, Issue: issue, Attempt: attempt, Action: action, InvalidatedGeneration: invalidated, Generation: generation, CleanupPhase: phase, PublishedHead: publishedHead, Manifest: manifest}
	if action != "dismissed" && !(action == "removed" && phase == "completed" && manifest == nil) {
		policy := agentruntime.EffectCleanupPolicy{Action: map[string]string{"archived": "archive", "abandoned": "abandon", "removed": "remove"}[action], PublishedHead: publishedHead}
		if manifest != nil {
			manifestSeen, logSeen, err := compatibilityResources(*manifest)
			if err != nil {
				return err
			}
			policy.CompatibilityManifestSeen, policy.CompatibilityLogSeen = manifestSeen, logSeen
		}
		tombstone.CleanupPolicy = &policy
		effectState := "pending"
		if phase == "completed" {
			effectState = "completed"
		}
		effect := runtimeEffectIntent{Action: string(agentruntime.EffectCleanup), Repository: repository, Issue: issue, Attempt: attempt, IssueGeneration: state.IssueGenerations[issueKey], AttemptGeneration: generation, IntentRevision: 1, State: effectState}
		if phase != "completed" {
			request := agentruntime.EffectRequest{Action: agentruntime.EffectCleanup, Attempt: operatorEffectAttempt(*manifest), Manifest: cloneManifest(*manifest), Runtime: agentruntime.EffectRuntime{Tmux: "tmux"}, Cleanup: policy}
			digest, err := agentruntime.EffectRequestDigest(request)
			if err != nil {
				return err
			}
			effect.IntentEpoch, effect.RequestDigest = 1, digest
		}
		effect.ID = runtimeEffectID(effect)
		state.Effects[effect.ID] = effect
		tombstone.EffectID = effect.ID
	}
	state.Tombstones[attemptKey] = tombstone
	return nil
}

func bindMigratedRemovalReceipts(state *runtimeOwnerState) error {
	bound := map[string]bool{}
	for index := range state.ControlReceipts {
		receipt := &state.ControlReceipts[index]
		if receipt.State != "pending" {
			continue
		}
		key := ownerAttemptKey(receipt.Request.Repository, receipt.Request.Issue, receipt.Request.Attempt)
		tombstone, ok := state.Tombstones[key]
		if receipt.Request.Action != "remove" || !ok || tombstone.Action != "removed" {
			return fmt.Errorf("legacy control receipt %s is pending without a v2 effect proof", receipt.Request.RequestID)
		}
		if bareCompletedRemoval(tombstone) {
			receipt.State, receipt.Phase, receipt.EffectID = "completed", operatorPhaseCompleted, ""
			receipt.Result = successfulOperatorResult(receipt.Request, 1)
			continue
		}
		if tombstone.EffectID == "" || tombstone.CleanupPhase == "completed" {
			return fmt.Errorf("legacy control receipt %s has invalid removal proof", receipt.Request.RequestID)
		}
		receipt.Phase, receipt.EffectID = operatorPhaseCleanupPending, tombstone.EffectID
		if tombstone.CleanupPhase == "cleanup-started" {
			receipt.Phase = operatorPhaseCleanupStarted
		}
		bound[tombstone.EffectID] = true
	}
	for key, tombstone := range state.Tombstones {
		if tombstone.Action != "removed" || tombstone.CleanupPhase == "completed" || bound[tombstone.EffectID] {
			continue
		}
		request := controlRequest{Version: controlVersion, RequestID: "migration-" + tombstone.EffectID, Repository: tombstone.Repository, Action: "remove", Issue: tombstone.Issue, Attempt: tombstone.Attempt, Confirm: true}
		phase := operatorPhaseCleanupPending
		if tombstone.CleanupPhase == "cleanup-started" {
			phase = operatorPhaseCleanupStarted
		}
		if err := appendOperatorReceipt(state, controlReceipt{Request: request, State: "pending", Phase: phase, EffectID: tombstone.EffectID}); err != nil {
			return fmt.Errorf("migrate removal %s receipt: %w", key, err)
		}
	}
	return nil
}

func writeRuntimeOwnerState(stateRoot, attemptRoot string, state runtimeOwnerState) error {
	if err := validateRuntimeOwnerState(state, attemptRoot, stateRoot, true); err != nil {
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
		return statePersistenceInstalledError{err: err}
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return statePersistenceInstalledError{err: err}
	}
	return nil
}

func validateRuntimeOwnerState(state runtimeOwnerState, attemptRoot, stateRoot string, persisted bool) error {
	if state.Version != runtimeOwnerStateVersion || strings.TrimSpace(state.Repository) == "" || state.IssueGenerations == nil || state.AttemptGenerations == nil || state.Attempts == nil || state.Observations == nil || state.Recoveries == nil || state.Tombstones == nil || state.Effects == nil || state.ControlReceipts == nil || persisted && (state.Epoch == 0 || state.Revision == 0) {
		return errors.New("runtime owner ledger is invalid")
	}
	if !boundedText(state.CycleDiagnostic, maxReconciliationStringBytes, false) || (state.CycleDiagnostic == "") != state.CycleDiagnosticAt.IsZero() {
		return errors.New("runtime owner cycle outcome is invalid")
	}
	if (state.CycleOutcomeEpoch == 0) != (state.CycleOutcomeID == 0) || (state.CycleOutcomeEpoch == 0) != (state.CycleOutcomeSource == 0) || state.CycleOutcomeEpoch > state.Epoch || state.CycleOutcomeSource > state.Revision {
		return errors.New("runtime owner cycle outcome identity is invalid")
	}
	for key, generation := range state.ControlGenerations {
		if generation < 2 || issueFromOwnerKey(key) < 1 || key != ownerIssueKey(state.Repository, issueFromOwnerKey(key)) {
			return errors.New("runtime owner control generation is invalid")
		}
	}
	for key, repair := range state.ControlRepairs {
		snapshot, err := internalgithub.ParseSnapshotComment(repair.Body, 1, 1)
		if err != nil || repair.Generation != controlGeneration(state, key) || snapshot.OwnerGeneration != repair.Generation {
			return errors.New("runtime owner control repair is invalid")
		}
	}
	for key, diagnostic := range state.LegacyReviewerQuarantines {
		issue := issueFromOwnerKey(key)
		if issue < 1 || key != ownerIssueKey(state.Repository, issue) || !boundedText(diagnostic, maxReconciliationStringBytes, true) {
			return errors.New("runtime owner legacy reviewer quarantine is invalid")
		}
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
	for key, status := range state.MachineStatuses {
		attemptKey := ownerAttemptKey(status.Repository, status.Issue, status.Attempt)
		attemptBound := (status.Source == "dependency" || status.Source == "destructive") && status.AttemptGeneration == 0 || status.AttemptGeneration > 0 && status.AttemptGeneration == state.AttemptGenerations[attemptKey]
		if key != ownerIssueKey(status.Repository, status.Issue) || status.Repository != state.Repository || status.Issue < 1 || status.Attempt < 1 ||
			status.IssueGeneration == 0 || status.IssueGeneration != state.IssueGenerations[key] || !attemptBound ||
			status.Sequence == 0 || status.AppliedSequence > status.Sequence || status.SourceSequence == 0 || !boundedText(status.SourceID, 128, true) ||
			!slices.Contains([]string{"worker", "dependency", "orchestrator", "destructive"}, status.Source) ||
			!slices.Contains([]string{"needs-attention", "clear"}, status.Status) || !boundedText(status.Reason, 1024, true) {
			return errors.New("runtime owner machine status is invalid")
		}
	}
	for key, record := range state.Attempts {
		if state.AttemptGenerations[key] != record.Generation || record.Generation == 0 || record.ObservationEpoch > state.Epoch || record.ObservationEpoch == 0 && record.LastCycleID != 0 || record.ObservationEpoch != 0 && record.LastCycleID == 0 || key != ownerAttemptKey(record.Manifest.Repository, record.Manifest.Issue, record.Manifest.Attempt) {
			return errors.New("runtime owner attempt record is invalid")
		}
		if err := validateOwnerManifest(state.Repository, attemptRoot, stateRoot, record.Manifest); err != nil {
			return err
		}
		if record.Manifest.WorkerProfileDigest != "" && record.Manifest.WorkerGeneration != record.Generation {
			stop, stopping := state.Effects[record.StopEffectID]
			if !stopping || record.Manifest.WorkerGeneration == ^uint64(0) || record.Manifest.WorkerGeneration+1 != record.Generation || stop.Action != string(agentruntime.EffectStop) || stop.State != "pending" || stop.Repository != record.Manifest.Repository || stop.Issue != record.Manifest.Issue || stop.Attempt != record.Manifest.Attempt || stop.AttemptGeneration != record.Generation {
				return errors.New("runtime owner worker confinement generation is invalid")
			}
		}
		if record.WorkerSeal != nil && !validWorkerSealSelection(stateRoot, record.Manifest, record.Generation, *record.WorkerSeal) {
			return errors.New("runtime owner worker seal selection is invalid")
		}
		if record.StopEffectID != "" {
			effect, ok := state.Effects[record.StopEffectID]
			if !ok || effect.Action != string(agentruntime.EffectStop) || effect.State != "pending" || effect.Repository != record.Manifest.Repository || effect.Issue != record.Manifest.Issue || effect.Attempt != record.Manifest.Attempt || effect.AttemptGeneration != record.Generation || effect.IssueGeneration != state.IssueGenerations[ownerIssueKey(effect.Repository, effect.Issue)] {
				return errors.New("runtime owner pending stop is unbound")
			}
		}
	}
	if err := validateReconciliationObservations(state); err != nil {
		return err
	}
	if err := validateRuntimePRRecoveries(state); err != nil {
		return err
	}
	for key, tombstone := range state.Tombstones {
		if key != ownerAttemptKey(tombstone.Repository, tombstone.Issue, tombstone.Attempt) || tombstone.Repository != state.Repository || !validTombstoneAction(tombstone.Action) || !validCleanupPhase(tombstone.CleanupPhase) || tombstone.Generation == 0 || tombstone.InvalidatedGeneration >= tombstone.Generation || state.AttemptGenerations[key] != tombstone.Generation || tombstone.Revision > state.Revision || persisted && tombstone.Revision == 0 {
			return errors.New("runtime owner tombstone is invalid")
		}
		if _, exists := state.Attempts[key]; exists {
			return errors.New("runtime owner tombstone conflicts with an attempt")
		}
		if tombstone.Manifest != nil {
			if err := validateOwnerManifest(state.Repository, attemptRoot, stateRoot, *tombstone.Manifest); err != nil || tombstone.Manifest.Issue != tombstone.Issue || tombstone.Manifest.Attempt != tombstone.Attempt {
				return errors.New("runtime owner tombstone manifest is invalid")
			}
			if tombstone.Manifest.WorkerProfileDigest != "" && tombstone.Manifest.WorkerGeneration != tombstone.InvalidatedGeneration {
				return errors.New("runtime owner tombstone confinement generation is invalid")
			}
		}
		if tombstone.InvalidatedHandoff != nil && (tombstone.Manifest == nil || !validHandoffCandidateInvalidation(*tombstone.InvalidatedHandoff, *tombstone.Manifest)) {
			return errors.New("runtime owner tombstone handoff invalidation is invalid")
		}
		if tombstone.InvalidatedStart != nil && (tombstone.Manifest == nil || !validStartCandidateInvalidation(*tombstone.InvalidatedStart, *tombstone.Manifest)) {
			return errors.New("runtime owner tombstone Start invalidation is invalid")
		}
		if tombstone.HandoffCompensated && tombstone.InvalidatedHandoff == nil {
			return errors.New("runtime owner handoff compensation has no candidate")
		}
		for effectID, outcome := range tombstone.ExternalOutcomes {
			effect, ok := state.Effects[effectID]
			decoded, decodeErr := hex.DecodeString(effectID)
			if decodeErr != nil || len(decoded) != 16 || outcome.Action == "" || outcome.PR < 0 || outcome.HeadSHA != "" && !preflightObjectID.MatchString(outcome.HeadSHA) {
				return errors.New("runtime owner invalidated external outcome is invalid")
			}
			if ok && (effect.State != "invalidated-resolved" || effect.Repository != tombstone.Repository || effect.Issue != tombstone.Issue || effect.Attempt != tombstone.Attempt || effect.AttemptGeneration != tombstone.InvalidatedGeneration || !validInvalidatedExternalOutcome(effect, outcome)) {
				return errors.New("runtime owner invalidated external outcome binding is invalid")
			}
		}
		if !validTombstoneCleanupPolicy(tombstone) {
			return errors.New("runtime owner tombstone cleanup policy is invalid")
		}
		if tombstone.ReviewerLeaseID != "" {
			bound := false
			for _, proof := range state.ReviewerProofs {
				if proof.Repository == tombstone.Repository && proof.Issue == tombstone.Issue && proof.Attempt == tombstone.Attempt && proof.EffectID == tombstone.ReviewerLeaseID && !proof.NeverRan && !proof.DeadProved {
					bound = true
				}
			}
			if !bound {
				return errors.New("runtime owner reviewer cleanup lease is unbound")
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
			if !ok || effect.Action != string(agentruntime.EffectCleanup) || effect.Repository != tombstone.Repository || effect.Issue != tombstone.Issue || effect.Attempt != tombstone.Attempt || effect.AttemptGeneration != tombstone.Generation || tombstone.CleanupPhase == "completed" && effect.State != "completed" || tombstone.CleanupPhase != "completed" && effect.State != "pending" {
				return errors.New("runtime owner tombstone effect is invalid")
			}
		} else if tombstone.Action != "dismissed" && !bareCompletedRemoval(tombstone) && !bareCompletedArchive(tombstone) {
			return errors.New("destructive tombstone lacks its cleanup effect")
		}
	}
	for key, effect := range state.Effects {
		maxRevision := state.Revision
		maxEpoch := state.Epoch
		if !persisted {
			maxRevision++
			maxEpoch++
		}
		issueKey, attemptKey := ownerIssueKey(effect.Repository, effect.Issue), ownerAttemptKey(effect.Repository, effect.Issue, effect.Attempt)
		receiptBoundCompletion := effect.State == "completed" && effectReferencedByReceipt(state, effect.ID)
		issueCurrent := state.IssueGenerations[issueKey] == effect.IssueGeneration || effectAuthorizedByTombstone(state, effect) || receiptBoundCompletion || invalidatedExternalEffect(state, effect)
		if effect.Reconciliation != nil {
			issueScoped := reconciliationEffectIssueScoped(*effect.Reconciliation)
			tombstone := state.Tombstones[attemptKey]
			invalidatedAttempt := (effect.State == "invalidated" || effect.State == "invalidated-resolved") && tombstone.InvalidatedGeneration == effect.AttemptGeneration
			attemptCurrent := issueScoped && effect.Attempt == 0 && effect.AttemptGeneration == 0 || !issueScoped && effect.Attempt > 0 && effect.AttemptGeneration > 0 && (state.AttemptGenerations[attemptKey] == effect.AttemptGeneration || receiptBoundCompletion || invalidatedAttempt)
			if key != effect.ID || effect.ID != runtimeEffectID(effect) || effect.Repository != state.Repository || effect.Issue < 1 || effect.Action == "" || effect.IntentRevision == 0 || effect.IntentRevision > maxRevision || effect.State != "pending" && effect.State != "completed" && effect.State != "invalidated" && effect.State != "invalidated-resolved" || !issueCurrent || !attemptCurrent || !validPersistedReconciliationEffect(state, effect) {
				return fmt.Errorf("runtime owner reconciliation effect intent %s (%s) is invalid: identity=%t issue_current=%t attempt_current=%t payload=%t", effect.ID, effect.Action, key == effect.ID && effect.ID == runtimeEffectID(effect), issueCurrent, attemptCurrent, validPersistedReconciliationEffect(state, effect))
			}
			continue
		}
		if effect.ReconciliationResult != nil || effect.ReviewerRevoked {
			return errors.New("runtime owner effect payload is invalid")
		}
		if key != effect.ID || effect.ID != runtimeEffectID(effect) || effect.Repository != state.Repository || effect.Issue < 1 || effect.Attempt < 1 || effect.Action == "" || effect.IssueGeneration == 0 || effect.AttemptGeneration == 0 || effect.IntentRevision == 0 || effect.IntentRevision > maxRevision || effect.State != "pending" && effect.State != "completed" || !issueCurrent || state.AttemptGenerations[attemptKey] != effect.AttemptGeneration && !receiptBoundCompletion {
			return errors.New("runtime owner effect intent is invalid")
		}
		if effect.RequestDigest != "" && (!agentruntime.ValidEffectRequestDigest(effect.RequestDigest) || effect.IntentEpoch == 0 || effect.IntentEpoch > maxEpoch || !validRuntimeEffectInput(agentruntime.EffectAction(effect.Action), effect.Reason) || (effect.Action == string(agentruntime.EffectReview)) != (effect.Review != nil)) {
			return errors.New("runtime owner typed effect intent is invalid")
		}
		if effect.SupersededReviewerGroupPID != 0 && (effect.SupersededReviewerID == "" || effect.SupersededReviewerGroupPID < 2) || effect.SupersededReviewerID != "" && (effect.Action != string(agentruntime.EffectStop) && effect.Action != string(agentruntime.EffectCleanup) || !validReviewerWaitChannel("review-"+effect.SupersededReviewerID) || !agentruntime.ValidReviewTarget(effect.SupersededReviewerMode, effect.SupersededReviewerTarget, effect.Repository, effect.Issue) || effect.SupersededReviewerIssueGeneration == 0 || effect.SupersededReviewerAttemptGeneration == 0 || effect.SupersededReviewerSessionRequested && !effect.SupersededReviewerGateProtocol || effect.SupersededReviewerGateProtocol && !validDigest(effect.SupersededReviewerRequestDigest)) || effect.SupersededReviewerID == "" && (effect.SupersededReviewerTarget != "" || effect.SupersededReviewerMode != "" || effect.SupersededReviewerIssueGeneration != 0 || effect.SupersededReviewerAttemptGeneration != 0 || effect.ReviewerStopped || effect.SupersededReviewerGateProtocol || effect.SupersededReviewerSessionRequested || effect.SupersededReviewerRequestDigest != "") {
			return errors.New("runtime owner superseded reviewer binding is invalid")
		}
		if effect.InvalidatedHandoff != nil {
			record, ok := state.Attempts[attemptKey]
			if effect.Action != string(agentruntime.EffectStop) || !ok || !validHandoffCandidateInvalidation(*effect.InvalidatedHandoff, record.Manifest) {
				return errors.New("runtime owner stop handoff invalidation is invalid")
			}
		}
		if effect.InvalidatedStart != nil {
			record, ok := state.Attempts[attemptKey]
			if effect.Action != string(agentruntime.EffectStop) || !ok || !validStartCandidateInvalidation(*effect.InvalidatedStart, record.Manifest) {
				return errors.New("runtime owner Stop Start invalidation is invalid")
			}
		}
		if effect.Action == string(agentruntime.EffectStart) && len(effect.StartCandidates) != 0 {
			if !validStartCandidates(effect.StartCandidates) || effect.StartGateNonce != effect.StartCandidates[len(effect.StartCandidates)-1].Nonce || effect.StartMayRun != effect.StartCandidates[len(effect.StartCandidates)-1].MayRun {
				return errors.New("runtime owner Start gate candidates are invalid")
			}
		} else if effect.StartGateNonce != "" || effect.StartMayRun || len(effect.StartCandidates) != 0 {
			return errors.New("runtime owner unexpected Start gate state")
		}
		if effect.RequestDigest != "" && effect.Action == string(agentruntime.EffectReview) {
			record, ok := state.Attempts[attemptKey]
			if !ok {
				return errors.New("runtime owner review effect has no attempt")
			}
			if _, err := agentruntime.ReviewEffectResult(attemptRoot, stateRoot, record.Manifest, *effect.Review); err != nil {
				return errors.New("runtime owner review effect transition is invalid")
			}
		}
	}
	for key, proof := range state.ReviewerProofs {
		if key != reviewerProofKey(proof.Repository, proof.Issue, proof.Attempt, proof.Mode, proof.Target) || proof.Repository != state.Repository || proof.Issue < 1 || proof.Attempt < 1 || !agentruntime.ValidReviewTarget(proof.Mode, proof.Target, proof.Repository, proof.Issue) || !validReviewerWaitChannel("review-"+proof.EffectID) || (proof.GroupPID < 2 && !(proof.GroupPID == 0 && (!proof.DeadProved || proof.NeverRan))) || proof.NeverRan && (proof.GroupPID != 0 || !proof.DeadProved || proof.LegacyUnverified) || proof.LegacyUnverified && proof.DeadProved || proof.IssueGeneration == 0 || proof.AttemptGeneration == 0 || proof.IssueGeneration > state.IssueGenerations[ownerIssueKey(proof.Repository, proof.Issue)] || proof.AttemptGeneration > state.AttemptGenerations[ownerAttemptKey(proof.Repository, proof.Issue, proof.Attempt)] {
			return errors.New("runtime owner reviewer process proof is invalid")
		}
	}
	if len(state.ControlReceipts) > maxControlReceipts {
		return errors.New("runtime owner control receipts are invalid")
	}
	seenReceipts := map[string]bool{}
	for _, receipt := range state.ControlReceipts {
		if seenReceipts[receipt.Request.RequestID] || !validControlRequest(receipt.Request, state.Repository) || receipt.State != "pending" && receipt.State != "completed" || receipt.State == "pending" && receipt.Result != nil || receipt.State == "completed" && (receipt.Result == nil || !validRecordedControlResult(*receipt.Result, receipt.Request)) || !validOperatorReceiptBinding(receipt) {
			return errors.New("runtime owner control receipt is invalid")
		}
		maxRevision := state.Revision
		if !persisted {
			maxRevision++
		}
		if receipt.Phase != "" && receipt.State == "completed" && (receipt.Result.OwnerRevision == 0 || receipt.Result.OwnerRevision > maxRevision) {
			return errors.New("runtime owner control receipt revision is invalid")
		}
		if receipt.EffectID != "" {
			effect, ok := state.Effects[receipt.EffectID]
			if !ok || effect.State != operatorReceiptEffectState(receipt) || !operatorReceiptMatchesEffect(receipt, effect) {
				return errors.New("runtime owner control receipt effect is invalid")
			}
		}
		if receipt.Phase == operatorPhaseHandoffCleanup {
			tombstone, ok := state.Tombstones[ownerAttemptKey(receipt.Request.Repository, receipt.Request.Issue, receipt.Request.Attempt)]
			if !ok || tombstone.Action != "dismissed" || tombstone.InvalidatedHandoff == nil || tombstone.HandoffCompensated {
				return errors.New("pending handoff compensation receipt is unbound")
			}
		}
		if receipt.Phase == operatorPhaseStartCleanup {
			tombstone, ok := state.Tombstones[ownerAttemptKey(receipt.Request.Repository, receipt.Request.Issue, receipt.Request.Attempt)]
			if !ok || tombstone.Action != "dismissed" || tombstone.InvalidatedStart == nil {
				return errors.New("pending start cleanup receipt is unbound")
			}
		}
		seenReceipts[receipt.Request.RequestID] = true
	}
	return nil
}

func validateOwnerManifest(repository, attemptRoot, stateRoot string, manifest agentruntime.Manifest) error {
	if manifest.Repository != repository || manifest.Issue < 1 || manifest.Attempt < 1 {
		return errors.New("runtime owner manifest is invalid")
	}
	if !filepath.IsAbs(stateRoot) {
		return errors.New("runtime owner state root is invalid")
	}
	return agentruntime.ValidateManifest(attemptRoot, stateRoot, manifest)
}

func runtimeOwnerAttemptRoot(stateRoot string) string {
	root := productionAttemptRoot(stateRoot)
	if canonical, err := filepath.EvalSymlinks(root); err == nil {
		return canonical
	}
	return filepath.Clean(root)
}

func newRuntimeOwnerState(repository string) runtimeOwnerState {
	return runtimeOwnerState{Version: runtimeOwnerStateVersion, Repository: repository, WorkerProfileDigest: config.WorkerProfileDigest(), ReviewerRevocationTracked: true, ReviewerSafetyMigrated: true, ExternalDispatchTracked: true, LegacyReviewerQuarantines: map[string]string{}, IssueGenerations: map[string]uint64{}, AttemptGenerations: map[string]uint64{}, Attempts: map[string]runtimeAttemptRecord{}, Observations: map[string]reconciliationObservation{}, Recoveries: map[string]runtimePRRecovery{}, Tombstones: map[string]runtimeTombstone{}, Effects: map[string]runtimeEffectIntent{}, ReviewerProofs: map[string]reviewerProcessProof{}, ControlReceipts: []controlReceipt{}, ControlGenerations: map[string]uint64{}, ControlRepairs: map[string]controlSnapshotRepair{}, MachineStatuses: map[string]machineStatusRecord{}}
}

func activeWorkerProfileDigest(state runtimeOwnerState) string {
	if validDigest(state.WorkerProfileDigest) {
		return state.WorkerProfileDigest
	}
	return config.WorkerProfileDigest()
}

func cloneRuntimeOwnerState(state runtimeOwnerState) runtimeOwnerState {
	clone := state
	clone.IssueGenerations = cloneMap(state.IssueGenerations)
	clone.AttemptGenerations = cloneMap(state.AttemptGenerations)
	clone.ControlGenerations = cloneMap(state.ControlGenerations)
	clone.ControlRepairs = make(map[string]controlSnapshotRepair, len(state.ControlRepairs))
	for key, repair := range state.ControlRepairs {
		clone.ControlRepairs[key] = repair
	}
	clone.MachineStatuses = make(map[string]machineStatusRecord, len(state.MachineStatuses))
	for key, status := range state.MachineStatuses {
		clone.MachineStatuses[key] = status
	}
	clone.Attempts = make(map[string]runtimeAttemptRecord, len(state.Attempts))
	for key, record := range state.Attempts {
		record.Manifest = cloneManifest(record.Manifest)
		if record.WorkerSeal != nil {
			selection := *record.WorkerSeal
			record.WorkerSeal = &selection
		}
		clone.Attempts[key] = record
	}
	clone.Observations = make(map[string]reconciliationObservation, len(state.Observations))
	for key, observation := range state.Observations {
		clone.Observations[key] = cloneReconciliationObservation(observation)
	}
	clone.Recoveries = make(map[string]runtimePRRecovery, len(state.Recoveries))
	for key, recovery := range state.Recoveries {
		recovery.State = clonePRState(recovery.State)
		clone.Recoveries[key] = recovery
	}
	clone.Tombstones = make(map[string]runtimeTombstone, len(state.Tombstones))
	for key, tombstone := range state.Tombstones {
		if tombstone.Manifest != nil {
			manifest := cloneManifest(*tombstone.Manifest)
			tombstone.Manifest = &manifest
		}
		if tombstone.ExternalOutcomes != nil {
			tombstone.ExternalOutcomes = maps.Clone(tombstone.ExternalOutcomes)
		}
		tombstone.CleanupPolicy = cloneCleanupPolicy(tombstone.CleanupPolicy)
		tombstone.InvalidatedHandoff = cloneHandoffInvalidation(tombstone.InvalidatedHandoff)
		tombstone.InvalidatedStart = cloneStartInvalidation(tombstone.InvalidatedStart)
		clone.Tombstones[key] = tombstone
	}
	clone.Effects = make(map[string]runtimeEffectIntent, len(state.Effects))
	for key, effect := range state.Effects {
		effect.Review = cloneReviewTransition(effect.Review)
		effect.Reconciliation = cloneReconciliationEffectRequest(effect.Reconciliation)
		effect.ReconciliationResult = cloneReconciliationEffectResult(effect.ReconciliationResult)
		effect.InvalidatedHandoff = cloneHandoffInvalidation(effect.InvalidatedHandoff)
		effect.InvalidatedStart = cloneStartInvalidation(effect.InvalidatedStart)
		effect.StartCandidates = slices.Clone(effect.StartCandidates)
		effect.GovernancePhases = slices.Clone(effect.GovernancePhases)
		clone.Effects[key] = effect
	}
	clone.ReviewerProofs = make(map[string]reviewerProcessProof, len(state.ReviewerProofs))
	for key, proof := range state.ReviewerProofs {
		clone.ReviewerProofs[key] = proof
	}
	clone.LegacyReviewerQuarantines = make(map[string]string, len(state.LegacyReviewerQuarantines))
	for key, diagnostic := range state.LegacyReviewerQuarantines {
		clone.LegacyReviewerQuarantines[key] = diagnostic
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

func cloneCleanupPolicy(policy *agentruntime.EffectCleanupPolicy) *agentruntime.EffectCleanupPolicy {
	if policy == nil {
		return nil
	}
	clone := *policy
	return &clone
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
	clone.Review = cloneReviewTransition(effect.Review)
	clone.Reconciliation = cloneReconciliationEffectRequest(effect.Reconciliation)
	clone.ReconciliationResult = cloneReconciliationEffectResult(effect.ReconciliationResult)
	clone.InvalidatedHandoff = cloneHandoffInvalidation(effect.InvalidatedHandoff)
	clone.InvalidatedStart = cloneStartInvalidation(effect.InvalidatedStart)
	clone.StartCandidates = slices.Clone(effect.StartCandidates)
	clone.GovernancePhases = slices.Clone(effect.GovernancePhases)
	return &clone
}

func cloneStartInvalidation(candidate *startCandidateInvalidation) *startCandidateInvalidation {
	if candidate == nil {
		return nil
	}
	clone := *candidate
	clone.Manifest = cloneManifest(clone.Manifest)
	clone.Candidates = slices.Clone(clone.Candidates)
	return &clone
}

func cloneHandoffInvalidation(candidate *handoffCandidateInvalidation) *handoffCandidateInvalidation {
	if candidate == nil {
		return nil
	}
	clone := *candidate
	clone.Manifest = cloneManifest(clone.Manifest)
	return &clone
}

func cloneReviewTransition(review *agentruntime.ReviewTransition) *agentruntime.ReviewTransition {
	if review == nil {
		return nil
	}
	clone := *review
	clone.Findings = slices.Clone(review.Findings)
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
	material := fmt.Sprintf("%s\x00%s\x00%d\x00%d\x00%d\x00%d\x00%d\x00%d\x00%s\x00%s", effect.Action, effect.Repository, effect.Issue, effect.Attempt, effect.IssueGeneration, effect.AttemptGeneration, effect.IntentRevision, effect.IntentEpoch, effect.RequestDigest, effect.Reason)
	if effect.SupersededReviewerID != "" {
		material += fmt.Sprintf("\x00%s\x00%s\x00%s\x00%d\x00%d", effect.SupersededReviewerID, effect.SupersededReviewerMode, effect.SupersededReviewerTarget, effect.SupersededReviewerIssueGeneration, effect.SupersededReviewerAttemptGeneration)
	}
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
		if !effectReferencedByReceipt(*state, id) && effect.Repository == repository && effect.Issue == issue && effect.Attempt == attempt {
			delete(state.Effects, id)
		}
	}
}

func invalidateAttemptEffects(state *runtimeOwnerState, repository string, issue, attempt int) {
	for id, effect := range state.Effects {
		if effectReferencedByReceipt(*state, id) || effect.Repository != repository || effect.Issue != issue || effect.Attempt != attempt {
			continue
		}
		if effect.Reconciliation != nil && reconciliationMutatesGitHub(effect.Reconciliation.Action) && effect.State == "pending" && effect.Dispatched {
			effect.State, effect.Diagnostic = "invalidated", ""
			state.Effects[id] = effect
			continue
		}
		delete(state.Effects, id)
	}
}

func pruneSupersededIssueEffects(state *runtimeOwnerState, repository string, issue int, nextGeneration uint64) error {
	for id, effect := range state.Effects {
		if effect.Repository != repository || effect.Issue != issue || effect.IssueGeneration >= nextGeneration || effectAuthorizedByTombstone(*state, effect) || invalidatedExternalEffect(*state, effect) {
			continue
		}
		if effectReferencedByReceipt(*state, id) {
			if effect.State == "pending" {
				return errStateConflict
			}
			continue
		}
		if pendingReviewerDeathUnproved(*state, effect) {
			return errStateConflict
		}
		delete(state.Effects, id)
	}
	return nil
}

func effectAuthorizedByTombstone(state runtimeOwnerState, effect runtimeEffectIntent) bool {
	tombstone, ok := state.Tombstones[ownerAttemptKey(effect.Repository, effect.Issue, effect.Attempt)]
	return ok && tombstone.EffectID == effect.ID && tombstone.Generation == effect.AttemptGeneration
}

func invalidatedExternalEffect(state runtimeOwnerState, effect runtimeEffectIntent) bool {
	if effect.Reconciliation == nil || !effect.Dispatched || effect.State != "invalidated" && effect.State != "invalidated-resolved" {
		return false
	}
	if effect.Attempt == 0 {
		if effect.Reconciliation.GitHubIssueUpdate != nil && effect.Reconciliation.GitHubIssueUpdate.Kind == githubIssueMachineStatus {
			status, ok := state.MachineStatuses[ownerIssueKey(effect.Repository, effect.Issue)]
			return ok && effect.Reconciliation.GitHubIssueUpdate.StatusSequence < status.Sequence
		}
		return effect.Reconciliation.ControlGeneration < controlGeneration(state, ownerIssueKey(effect.Repository, effect.Issue))
	}
	tombstone, ok := state.Tombstones[ownerAttemptKey(effect.Repository, effect.Issue, effect.Attempt)]
	return ok && tombstone.InvalidatedGeneration == effect.AttemptGeneration
}
