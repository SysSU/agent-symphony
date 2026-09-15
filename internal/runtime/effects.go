package runtime

import (
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
	"regexp"
	"slices"
	"strings"
	"syscall"
	"time"

	internalgithub "github.com/SysSU/agent-symphony/internal/github"
)

// EffectAction names one generation-bound runtime operation.
type EffectAction string

const (
	EffectPrepare EffectAction = "prepare"
	EffectStart   EffectAction = "start"
	EffectMonitor EffectAction = "monitor"
	EffectStop    EffectAction = "stop"
	EffectReview  EffectAction = "review"
	EffectHandoff EffectAction = "handoff"
	EffectCleanup EffectAction = "cleanup"
)

// EffectIdentity binds slow work to the state that authorized it.
type EffectIdentity struct {
	Repository        string
	Issue             int
	Attempt           int
	Epoch             uint64
	SourceRevision    uint64
	IssueGeneration   uint64
	AttemptGeneration uint64
	EffectID          string
	RequestDigest     string
}

// ReviewTransition is the owner-approved lifecycle result of review work.
type ReviewTransition struct {
	State               string
	Mode                string
	Target              string
	Base                string
	Head                string
	Snapshot            string
	Session             string
	Findings            []string
	HandoffQueued       bool
	HandoffAcknowledged bool
}

// EffectRequest is immutable input for external runtime work. Eligible is a
// captured owner decision; callbacks are rejected because they are not a snapshot.
type EffectRequest struct {
	Identity             EffectIdentity
	Action               EffectAction
	Attempt              Attempt
	Manifest             Manifest
	Runtime              EffectRuntime
	Eligible             bool
	Reason               string
	Review               ReviewTransition
	Cleanup              EffectCleanupPolicy
	CandidateLaunchToken string
	GateNonce            string
}

// EffectCleanupPolicy binds destructive authorization to one closed resource
// policy. Exact paths remain part of the immutable manifest identity.
type EffectCleanupPolicy struct {
	Action                    string
	PublishedHead             string
	CompatibilityManifestSeen bool
	CompatibilityLogSeen      bool
}

// EffectRuntime is the process configuration and environment snapshot used by
// one immutable request. It is hashed into the intent but never persisted.
type EffectRuntime struct {
	Source      string
	Git         string
	Tmux        string
	Helper      string
	StopWait    time.Duration
	Environment []string
}

// EffectResult carries the unchanged authorization identity back to the owner.
type EffectResult struct {
	Identity    EffectIdentity
	Action      EffectAction
	Manifest    Manifest
	Disposition EffectResultDisposition
}

// EffectResultDisposition distinguishes a result that is safe to commit from
// an ambiguous partial operation that must remain pending.
type EffectResultDisposition string

const (
	EffectResultAmbiguous EffectResultDisposition = "ambiguous"
	EffectResultReady     EffectResultDisposition = "ready"
)

// EffectDisposition reports how restart should handle a pending effect.
type EffectDisposition string

const (
	EffectVerified EffectDisposition = "verified"
	EffectRetry    EffectDisposition = "retry"
	EffectPending  EffectDisposition = "pending"
	EffectRotate   EffectDisposition = "rotate"
)

// EffectVerification carries a reconstructed result only when external state
// proves the intended effect completed.
type EffectVerification struct {
	Disposition EffectDisposition
	Result      *EffectResult
}

// EffectExecutor performs runtime I/O without reading or writing authoritative
// manifests. The caller owns command sequencing and result application.
type EffectExecutor struct {
	Runtime         *Runtime
	Cleanup         func(context.Context, EffectRequest) error
	VerifyCleanup   func(context.Context, EffectRequest) (bool, error)
	AuthorizeLaunch func(context.Context, EffectRequest) error
}

// BindRequest captures all runtime inputs before intent persistence.
func (e EffectExecutor) BindRequest(request EffectRequest) (EffectRequest, error) {
	if e.Runtime == nil {
		return EffectRequest{}, errors.New("runtime effect executor is missing")
	}
	config, err := effectRuntimeSnapshot(e.Runtime, request)
	if err != nil {
		return EffectRequest{}, err
	}
	request.Runtime = config
	return request, nil
}

func (e EffectExecutor) Execute(ctx context.Context, request EffectRequest) (EffectResult, error) {
	request = cloneEffectRequest(request)
	if err := e.validate(request); err != nil {
		return EffectResult{}, err
	}
	r := e.runtimeFor(request.Runtime)
	manifest := request.Manifest
	var err error
	switch request.Action {
	case EffectPrepare:
		manifest, err = r.prepareEffect(ctx, request)
	case EffectStart:
		manifest, err = r.startEffect(ctx, request, e.AuthorizeLaunch)
	case EffectMonitor:
		manifest, err = r.monitorEffect(ctx, request)
	case EffectStop:
		manifest, err = r.stopEffect(ctx, request)
	case EffectReview:
		manifest, err = r.reviewEffect(request)
	case EffectHandoff:
		manifest, err = r.handoffEffect(ctx, request, e.AuthorizeLaunch)
	case EffectCleanup:
		if e.Cleanup == nil || e.VerifyCleanup == nil {
			err = errors.New("runtime cleanup executor is missing")
		} else if err = e.Cleanup(ctx, request); err == nil {
			var complete bool
			complete, err = e.VerifyCleanup(ctx, request)
			if err == nil && !complete {
				err = ErrRuntimeResourcesRemain
			}
		}
	}
	result := EffectResult{Identity: request.Identity, Action: request.Action, Manifest: manifest, Disposition: EffectResultAmbiguous}
	if err == nil || definitiveFailedEffect(request.Action, manifest) {
		result.Disposition = EffectResultReady
		if request.Action != EffectMonitor {
			if markerErr := r.writeEffectMarker(result); markerErr != nil {
				result.Disposition = EffectResultAmbiguous
				err = errors.Join(err, markerErr)
			}
		}
	}
	return result, err
}

// VerifyPending checks ambiguous work after restart without repeating the side
// effect. Retry means the owner may safely reconstruct and dispatch the request.
func (e EffectExecutor) VerifyPending(ctx context.Context, request EffectRequest) (EffectVerification, error) {
	request = cloneEffectRequest(request)
	legacyHandoffWithoutHelper := request.Action == EffectHandoff && request.Runtime.Helper == ""
	if err := e.validateIdentity(request.Action, request.Identity); err != nil {
		return EffectVerification{}, err
	}
	verified := func(manifest Manifest) EffectVerification {
		result := EffectResult{Identity: request.Identity, Action: request.Action, Manifest: manifest, Disposition: EffectResultReady}
		return EffectVerification{Disposition: EffectVerified, Result: &result}
	}
	retry := EffectVerification{Disposition: EffectRetry}
	if request.Action != EffectMonitor {
		marked, err := e.Runtime.readEffectMarker(request.Identity, request.Action)
		if err != nil {
			return EffectVerification{}, err
		}
		if marked != nil {
			marked.Identity = request.Identity
			return EffectVerification{Disposition: EffectVerified, Result: marked}, nil
		}
	}
	request, err := e.BindRequest(request)
	if err != nil {
		return EffectVerification{}, err
	}
	if legacyHandoffWithoutHelper {
		// Keep the old persisted digest intact. Such an intent may reuse an
		// exact bound pane, but cannot create a replacement worker.
		request.Runtime.Helper = ""
	}
	if err := e.validate(request); err != nil {
		return EffectVerification{}, err
	}
	if request.Action == EffectMonitor {
		return retry, nil
	}
	r := e.runtimeFor(request.Runtime)
	switch request.Action {
	case EffectPrepare:
		if err := r.verifyPrepared(ctx, request.Manifest); err == nil {
			return verified(request.Manifest), nil
		} else if errors.Is(err, ErrWorktreeMissing) {
			return retry, nil
		} else {
			return EffectVerification{}, err
		}
	case EffectStart, EffectHandoff:
		live, err := r.session(ctx, request.Manifest.Session)
		if err != nil {
			return EffectVerification{}, err
		}
		if live {
			candidate := request.Manifest
			if request.Action == EffectStart {
				candidate.LaunchID = request.GateNonce
				if candidate.LaunchID == "" {
					return EffectVerification{Disposition: EffectPending}, nil
				}
			} else if request.CandidateLaunchToken != "" {
				candidate.LaunchToken, candidate.LaunchID = request.CandidateLaunchToken, request.Identity.EffectID
			}
			binding, pane, err := r.observeBound(ctx, candidate)
			if err != nil {
				return EffectVerification{Disposition: EffectPending}, nil
			}
			if request.Action == EffectStart {
				entered, err := ImplementationGateEntered(candidate, binding)
				if err != nil || entered {
					return EffectVerification{Disposition: EffectPending}, nil
				}
				status, err := r.guardedBoundResult(ctx, binding, pane, "display-message -p -t "+pane.PaneID+" #{pane_dead}", nil)
				if err != nil || strings.TrimSpace(status.Output) != "0" {
					return EffectVerification{Disposition: EffectPending}, nil
				}
				return retry, nil
			}
			return EffectVerification{Disposition: EffectPending}, nil
		}
		if request.Action == EffectStart {
			// The owner may rotate only a never-permitted candidate. A missing
			// name alone never authorizes replaying this candidate.
			if request.GateNonce != "" {
				return EffectVerification{Disposition: EffectRotate}, nil
			}
			return EffectVerification{Disposition: EffectPending}, nil
		}
		return retry, nil
	case EffectStop:
		live, err := r.session(ctx, request.Manifest.Session)
		if err != nil {
			return EffectVerification{}, err
		}
		if !live {
			return verified(cancelledEffect(request.Manifest, request.Reason)), nil
		}
		return retry, nil
	case EffectReview:
		return EffectVerification{Disposition: EffectPending}, nil
	case EffectCleanup:
		if complete, err := e.VerifyCleanup(ctx, request); err == nil && complete {
			return verified(request.Manifest), nil
		} else if err == nil {
			return retry, nil
		} else {
			return EffectVerification{}, err
		}
	default:
		return EffectVerification{}, errors.New("unknown runtime effect")
	}
}

type effectResultMarker struct {
	Version int          `json:"version"`
	Result  EffectResult `json:"result"`
}

func (r *Runtime) writeEffectMarker(result EffectResult) (returnErr error) {
	directory := filepath.Join(r.StateRoot, "runtime-effects")
	if err := mkdirBelow(r.StateRoot, directory, 0o700); err != nil {
		return err
	}
	path, err := effectMarkerPath(directory, result.Identity.EffectID)
	if err != nil {
		return err
	}
	body, err := json.Marshal(effectResultMarker{Version: 1, Result: result})
	if err != nil || len(body)+1 > 1<<20 {
		return errors.New("runtime effect result marker is too large")
	}
	body = append(body, '\n')
	temporary, err := os.CreateTemp(directory, ".effect-*")
	if err != nil {
		return err
	}
	name := temporary.Name()
	defer func() { returnErr = errors.Join(returnErr, removeEffectMarkerTemporary(directory, name)) }()
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return err
	}
	if _, err := temporary.Write(body); err != nil {
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
	if err := os.Link(name, path); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return err
		}
		existing, readErr := r.readEffectMarker(result.Identity, result.Action)
		if readErr != nil || !reflect.DeepEqual(existing, &result) {
			return errors.New("runtime effect result marker is immutable")
		}
	}
	dir, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func removeEffectMarkerTemporary(directory, path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	dir, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func (r *Runtime) readEffectMarker(identity EffectIdentity, action EffectAction) (*EffectResult, error) {
	directory := filepath.Join(r.StateRoot, "runtime-effects")
	path, err := effectMarkerPath(directory, identity.EffectID)
	if err != nil {
		return nil, err
	}
	listed, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
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
	if err != nil {
		return nil, err
	}
	if !os.SameFile(listed, info) || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o600 || !runtimeOwnedByCurrentUser(info) || info.Size() < 2 || info.Size() > 1<<20 {
		return nil, errors.New("runtime effect marker is unsafe")
	}
	body, err := io.ReadAll(io.LimitReader(file, 1<<20+1))
	if err != nil {
		return nil, err
	}
	if len(body) > 1<<20 {
		return nil, errors.New("runtime effect marker is too large")
	}
	var marker effectResultMarker
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&marker) != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return nil, errors.New("runtime effect marker is invalid")
	}
	result, stored := marker.Result, marker.Result.Identity
	if marker.Version != 1 || result.Disposition != EffectResultReady || result.Action != action || stored.Repository != identity.Repository || stored.Issue != identity.Issue || stored.Attempt != identity.Attempt || stored.Epoch != identity.Epoch || stored.EffectID != identity.EffectID || stored.RequestDigest != identity.RequestDigest || stored.SourceRevision != identity.SourceRevision || stored.IssueGeneration != identity.IssueGeneration || stored.AttemptGeneration != identity.AttemptGeneration {
		return nil, errors.New("runtime effect marker conflicts with its intent")
	}
	if result.Manifest.Repository != identity.Repository || result.Manifest.Issue != identity.Issue || result.Manifest.Attempt != identity.Attempt || ValidateManifest(r.Root, r.StateRoot, result.Manifest) != nil {
		return nil, errors.New("runtime effect marker contains an invalid result")
	}
	return &result, nil
}

// ReclaimOrphanEffectMarkers removes only fully validated immutable proofs
// that have no pending authoritative effect. It validates the whole directory
// before deleting anything so malformed entries fail closed.
func (r *Runtime) ReclaimOrphanEffectMarkers(pending map[string]bool) error {
	directory := filepath.Join(r.StateRoot, "runtime-effects")
	if err := rejectSymlinkPath(r.StateRoot, directory, true); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	info, err := os.Lstat(directory)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o700 || !runtimeOwnedByCurrentUser(info) {
		return errors.New("runtime effect marker directory is unsafe")
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		return err
	}
	var remove []string
	for _, entry := range entries {
		name := entry.Name()
		if effectMarkerTemporaryName(name) {
			path := filepath.Join(directory, name)
			if err := validateEffectMarkerTemporary(path); err != nil {
				return err
			}
			remove = append(remove, path)
			continue
		}
		if entry.IsDir() || entry.Type()&os.ModeSymlink != 0 || !strings.HasSuffix(name, ".done") {
			return errors.New("runtime effect marker directory contains an unsafe entry")
		}
		id := strings.TrimSuffix(name, ".done")
		if _, err := effectMarkerPath(directory, id); err != nil {
			return err
		}
		path := filepath.Join(directory, name)
		file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
		if err != nil {
			return err
		}
		listed, statErr := os.Lstat(path)
		opened, openErr := file.Stat()
		body, readErr := io.ReadAll(io.LimitReader(file, 1<<20+1))
		closeErr := file.Close()
		if statErr != nil || openErr != nil || readErr != nil || closeErr != nil || !os.SameFile(listed, opened) || !opened.Mode().IsRegular() || opened.Mode().Perm() != 0o600 || !runtimeOwnedByCurrentUser(opened) || len(body) > 1<<20 {
			return errors.New("runtime effect marker is unsafe")
		}
		var marker effectResultMarker
		decoder := json.NewDecoder(strings.NewReader(string(body)))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&marker) != nil || decoder.Decode(&struct{}{}) != io.EOF || marker.Version != 1 || marker.Result.Identity.EffectID != id {
			return errors.New("runtime effect marker is invalid")
		}
		if _, err := r.readEffectMarker(marker.Result.Identity, marker.Result.Action); err != nil {
			return err
		}
		if !pending[id] {
			remove = append(remove, path)
		}
	}
	for _, path := range remove {
		if err := os.Remove(path); err != nil {
			return err
		}
	}
	if len(remove) == 0 {
		return nil
	}
	dir, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func effectMarkerTemporaryName(name string) bool {
	suffix := strings.TrimPrefix(name, ".effect-")
	if suffix == "" || suffix == name {
		return false
	}
	for _, character := range suffix {
		if character < '0' || character > '9' {
			return false
		}
	}
	return true
}

func validateEffectMarkerTemporary(path string) error {
	listed, err := os.Lstat(path)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	opened, statErr := file.Stat()
	closeErr := file.Close()
	if statErr != nil || closeErr != nil || !os.SameFile(listed, opened) || !opened.Mode().IsRegular() || opened.Mode()&os.ModeSymlink != 0 || opened.Mode().Perm() != 0o600 || !runtimeOwnedByCurrentUser(opened) {
		return errors.New("runtime effect marker temporary file is unsafe")
	}
	return nil
}

func runtimeOwnedByCurrentUser(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && int(stat.Uid) == os.Getuid()
}

// RemoveEffectMarker reclaims completion proof only after the owner has
// durably committed that completion. A missing marker is already reclaimed.
func (r *Runtime) RemoveEffectMarker(identity EffectIdentity) error {
	directory := filepath.Join(r.StateRoot, "runtime-effects")
	if err := rejectSymlinkPath(r.StateRoot, directory, true); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	info, err := os.Lstat(directory)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o700 || !runtimeOwnedByCurrentUser(info) {
		return errors.New("runtime effect marker directory is unsafe")
	}
	path, err := effectMarkerPath(directory, identity.EffectID)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	dir, err := os.Open(directory)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func effectMarkerPath(directory, effectID string) (string, error) {
	decoded, err := hex.DecodeString(effectID)
	if err != nil || len(decoded) != 16 {
		return "", errors.New("runtime effect ID is invalid")
	}
	return filepath.Join(directory, effectID+".done"), nil
}

// ErrRuntimeResourcesRemain reports that cleanup is safe to retry but incomplete.
var ErrRuntimeResourcesRemain = errors.New("runtime resources remain")

func (e EffectExecutor) validate(request EffectRequest) error {
	if err := e.validateIdentity(request.Action, request.Identity); err != nil {
		return err
	}
	identity := request.Identity
	digest, err := EffectRequestDigest(request)
	if err != nil || digest != identity.RequestDigest {
		return errors.New("runtime effect request digest does not match its intent")
	}
	if identity.Repository != request.Manifest.Repository || identity.Issue != request.Manifest.Issue || identity.Attempt != request.Manifest.Attempt {
		return errors.New("runtime effect attempt does not match manifest")
	}
	return e.ValidateRequest(request)
}

// ValidateRequest rejects malformed work before its durable intent is committed.
// It performs no external I/O.
func (e EffectExecutor) ValidateRequest(request EffectRequest) error {
	if e.Runtime == nil || !slices.Contains([]EffectAction{EffectPrepare, EffectStart, EffectMonitor, EffectStop, EffectReview, EffectHandoff, EffectCleanup}, request.Action) {
		return errors.New("unknown runtime effect")
	}
	if len(request.Reason) > 4096 || strings.ContainsRune(request.Reason, 0) || request.Action == EffectStop && strings.TrimSpace(request.Reason) == "" || request.Action != EffectStop && request.Reason != "" {
		return errors.New("runtime effect reason is invalid")
	}
	if request.Attempt.Eligible != nil {
		return errors.New("runtime effect eligibility must be captured")
	}
	if request.Attempt.Repository != request.Manifest.Repository || request.Attempt.Issue != request.Manifest.Issue || request.Attempt.Number != request.Manifest.Attempt || request.Attempt.BaseSHA != request.Manifest.BaseSHA {
		return errors.New("runtime effect attempt does not match manifest")
	}
	if err := ValidateManifest(e.Runtime.Root, e.Runtime.StateRoot, request.Manifest); err != nil {
		return err
	}
	if request.Action == EffectHandoff && request.Manifest.Version == boundManifestVersion {
		if !ValidLaunchToken(request.CandidateLaunchToken) || request.CandidateLaunchToken == request.Manifest.LaunchToken {
			return errors.New("handoff candidate launch token is invalid")
		}
	} else if request.CandidateLaunchToken != "" {
		return errors.New("unexpected candidate launch token")
	}
	if !validEffectRuntime(request) {
		return errors.New("runtime effect configuration is invalid")
	}
	if (request.Action == EffectPrepare || request.Action == EffectStart) && strings.TrimSpace(request.Runtime.Helper) == "" {
		return errors.New("bound attempt helper is required")
	}
	switch request.Action {
	case EffectPrepare:
		if request.Manifest.State != "preparing" || !request.Eligible || invalidEffectLaunchInput(request) {
			return errors.New("prepare effect input is invalid")
		}
	case EffectStart:
		if request.Manifest.State != "preparing" || !request.Eligible || invalidEffectLaunchInput(request) {
			return errors.New("start effect input is invalid")
		}
	case EffectMonitor:
		if request.Manifest.State != "running" || !request.Eligible {
			return errors.New("monitor effect input is invalid")
		}
	case EffectStop:
		if request.Manifest.State != "preparing" && request.Manifest.State != "running" {
			return errors.New("stop effect input is invalid")
		}
	case EffectReview:
		if _, err := e.Runtime.reviewEffect(request); err != nil {
			return err
		}
	case EffectHandoff:
		if request.Manifest.State != "completed" && request.Manifest.State != "running" || !request.Eligible {
			return errors.New("handoff effect input is invalid")
		}
	case EffectCleanup:
		if e.Cleanup == nil || e.VerifyCleanup == nil || !validEffectCleanupPolicy(request.Cleanup) {
			return errors.New("cleanup effect policy is invalid")
		}
	}
	if request.Action != EffectCleanup && request.Cleanup != (EffectCleanupPolicy{}) {
		return errors.New("runtime effect cleanup policy is invalid")
	}
	return nil
}

var effectObjectID = regexp.MustCompile(`^[0-9a-f]{40,64}$`)

func validEffectCleanupPolicy(policy EffectCleanupPolicy) bool {
	switch policy.Action {
	case "archive", "abandon":
		return policy.PublishedHead == ""
	case "remove":
		return effectObjectID.MatchString(policy.PublishedHead)
	default:
		return false
	}
}

func invalidEffectLaunchInput(request EffectRequest) bool {
	return len(request.Attempt.Command) == 0 || strings.TrimSpace(request.Attempt.Command[0]) == "" ||
		request.Attempt.Interactive && strings.TrimSpace(request.Attempt.Context) == "" ||
		strings.TrimSpace(request.Runtime.Helper) == ""
}

func (e EffectExecutor) validateIdentity(action EffectAction, identity EffectIdentity) error {
	if e.Runtime == nil {
		return errors.New("runtime effect executor is missing")
	}
	if identity.Repository == "" || identity.Issue < 1 || identity.Attempt < 1 || identity.Epoch == 0 || identity.SourceRevision == 0 || identity.IssueGeneration == 0 || identity.AttemptGeneration == 0 || strings.TrimSpace(identity.EffectID) == "" || !ValidEffectRequestDigest(identity.RequestDigest) {
		return errors.New("runtime effect identity is incomplete")
	}
	if !slices.Contains([]EffectAction{EffectPrepare, EffectStart, EffectMonitor, EffectStop, EffectReview, EffectHandoff, EffectCleanup}, action) {
		return errors.New("unknown runtime effect")
	}
	return nil
}

// EffectRequestDigest binds authorization to complete immutable input without
// persisting command context or environment values that may contain secrets.
func EffectRequestDigest(request EffectRequest) (string, error) {
	material := struct {
		Action  EffectAction
		Attempt struct {
			Repository string
			Issue      int
			Number     int
			BaseSHA    string
		}
		Launch *struct {
			Context     string
			Command     []string
			Interactive bool
		}
		Manifest             Manifest
		Runtime              EffectRuntime
		Eligible             bool
		Reason               string
		Review               ReviewTransition
		Cleanup              EffectCleanupPolicy
		CandidateLaunchToken string `json:"CandidateLaunchToken,omitempty"`
	}{Action: request.Action, Manifest: request.Manifest, Runtime: request.Runtime, CandidateLaunchToken: request.CandidateLaunchToken}
	material.Attempt.Repository, material.Attempt.Issue = request.Attempt.Repository, request.Attempt.Issue
	material.Attempt.Number, material.Attempt.BaseSHA = request.Attempt.Number, request.Attempt.BaseSHA
	if slices.Contains([]EffectAction{EffectPrepare, EffectStart}, request.Action) {
		material.Launch = &struct {
			Context     string
			Command     []string
			Interactive bool
		}{request.Attempt.Context, request.Attempt.Command, request.Attempt.Interactive}
	}
	if slices.Contains([]EffectAction{EffectPrepare, EffectStart, EffectMonitor, EffectHandoff}, request.Action) {
		material.Eligible = request.Eligible
	}
	if request.Action == EffectStop {
		material.Reason = request.Reason
	}
	if request.Action == EffectReview {
		material.Review = request.Review
	}
	if request.Action == EffectCleanup {
		material.Cleanup = request.Cleanup
	}
	body, err := json.Marshal(material)
	if err != nil {
		return "", err
	}
	if len(body) > 1<<20 {
		return "", errors.New("runtime effect request exceeds 1 MiB")
	}
	digest := sha256.Sum256(body)
	return hex.EncodeToString(digest[:]), nil
}

func ValidEffectRequestDigest(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size
}

// PreparingManifest creates the deterministic manifest an owner commits before
// dispatching a prepare effect. The supplied roots must already be canonical.
func PreparingManifest(root, stateRoot string, attempt Attempt, now time.Time) (Manifest, error) {
	manifest, err := attemptIdentity(root, attempt)
	if err != nil {
		return Manifest{}, err
	}
	if !filepath.IsAbs(stateRoot) || filepath.Clean(stateRoot) != stateRoot {
		return Manifest{}, errors.New("runtime state root must be canonical and absolute")
	}
	manifest.LogPath = filepath.Join(stateRoot, "attempts", internalgithub.RepositoryIdentifier(attempt.Repository), fmt.Sprintf("%d-%d", attempt.Issue, attempt.Number), "agent.log")
	manifest.State, manifest.Interactive = "preparing", attempt.Interactive
	manifest.CreatedAt, manifest.UpdatedAt = now.UTC(), now.UTC()
	manifest.Version = boundManifestVersion
	manifest.LaunchToken, err = newLaunchToken()
	if err != nil {
		return Manifest{}, err
	}
	if err := ValidateManifest(root, stateRoot, manifest); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

func cloneEffectRequest(request EffectRequest) EffectRequest {
	request.Attempt.Command = slices.Clone(request.Attempt.Command)
	request.Attempt.Env = slices.Clone(request.Attempt.Env)
	request.Manifest.ReviewFindings = slices.Clone(request.Manifest.ReviewFindings)
	request.Review.Findings = slices.Clone(request.Review.Findings)
	request.Runtime.Environment = slices.Clone(request.Runtime.Environment)
	return request
}

func effectRuntimeSnapshot(r *Runtime, request EffectRequest) (EffectRuntime, error) {
	var config EffectRuntime
	switch request.Action {
	case EffectPrepare:
		config.Source, config.Git, config.Tmux, config.Helper = r.Source, r.git(), r.tmux(), r.Helper
	case EffectStart:
		config.Tmux, config.Helper = r.tmux(), r.Helper
	case EffectMonitor:
		config.Tmux = r.tmux()
	case EffectStop:
		config.Tmux, config.StopWait = r.tmux(), r.StopWait
	case EffectHandoff:
		config.Tmux, config.Helper = r.tmux(), r.Helper
		if request.Manifest.State == "completed" {
			config.Source, config.Git = r.Source, r.git()
		}
	case EffectCleanup:
		config.Tmux = r.tmux()
	case EffectReview:
		return config, nil
	default:
		return EffectRuntime{}, errors.New("unknown runtime effect")
	}
	if slices.Contains([]EffectAction{EffectPrepare, EffectStart, EffectMonitor}, request.Action) {
		environment, err := r.agentEnvironment(request.Manifest.Repository, request.Attempt.Env...)
		if err != nil {
			return EffectRuntime{}, fmt.Errorf("build worker environment: %w", err)
		}
		config.Environment = environment
	} else if request.Action == EffectHandoff {
		environment, err := r.agentEnvironment(request.Manifest.Repository)
		if err != nil {
			return EffectRuntime{}, fmt.Errorf("build worker environment: %w", err)
		}
		config.Environment = environment
	}
	return config, nil
}

func (e EffectExecutor) runtimeFor(config EffectRuntime) *Runtime {
	return &Runtime{
		Root: e.Runtime.Root, StateRoot: e.Runtime.StateRoot,
		Source: config.Source, Git: config.Git, Tmux: config.Tmux, Helper: config.Helper, StopWait: config.StopWait,
		Runner: e.Runtime.Runner, VerifyWorker: e.Runtime.VerifyWorker,
	}
}

func validEffectRuntime(request EffectRequest) bool {
	action, config := request.Action, request.Runtime
	environment := len(config.Environment) > 0
	switch action {
	case EffectPrepare:
		return config.Source != "" && config.Git != "" && config.Tmux != "" && environment && config.StopWait == 0
	case EffectStart:
		return config.Source == "" && config.Git == "" && config.Tmux != "" && environment && config.StopWait == 0
	case EffectMonitor:
		return config.Source == "" && config.Git == "" && config.Tmux != "" && config.Helper == "" && environment && config.StopWait == 0
	case EffectStop:
		return config.Source == "" && config.Git == "" && config.Tmux != "" && config.Helper == "" && !environment
	case EffectHandoff:
		source := config.Source == "" && config.Git == "" && request.Manifest.State == "running" || config.Source != "" && config.Git != "" && request.Manifest.State == "completed"
		return source && config.Tmux != "" && environment && config.StopWait == 0
	case EffectCleanup:
		return config.Source == "" && config.Git == "" && config.Tmux != "" && config.Helper == "" && !environment && config.StopWait == 0
	case EffectReview:
		return config.Source == "" && config.Git == "" && config.Tmux == "" && config.Helper == "" && config.StopWait == 0 && !environment
	default:
		return false
	}
}

func effectEnvironment(request EffectRequest) []string {
	return slices.Clone(request.Runtime.Environment)
}

func (r *Runtime) prepareEffect(ctx context.Context, request EffectRequest) (Manifest, error) {
	if !request.Eligible {
		return request.Manifest, errors.New("attempt is no longer eligible")
	}
	if err := r.verifyEffectWorker(ctx); err != nil {
		return request.Manifest, err
	}
	if len(request.Attempt.Command) == 0 || strings.TrimSpace(request.Attempt.Command[0]) == "" {
		return request.Manifest, errors.New("attempt command is required")
	}
	if request.Attempt.Interactive && strings.TrimSpace(request.Attempt.Context) == "" {
		return request.Manifest, errors.New("interactive attempt context is required")
	}
	if strings.TrimSpace(r.Helper) == "" {
		return request.Manifest, errors.New("bound attempt helper is required")
	}
	if len(effectEnvironment(request)) == 0 {
		return request.Manifest, errors.New("worker environment is missing")
	}
	manifest := request.Manifest
	if manifest.State != "preparing" {
		return manifest, errors.New("prepare effect requires preparing state")
	}
	if err := r.rejectCaseCollision(manifest.Repository); err != nil {
		return manifest, err
	}
	if _, err := os.Lstat(manifest.Worktree); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			return manifest, fmt.Errorf("worktree already exists: %s", manifest.Worktree)
		}
		return manifest, err
	}
	if _, err := os.Lstat(ResultPath(manifest.Worktree)); err == nil {
		return manifest, fmt.Errorf("worker result already exists: %s", ResultPath(manifest.Worktree))
	} else if !errors.Is(err, os.ErrNotExist) {
		return manifest, err
	}
	if live, err := r.session(ctx, manifest.Session); err != nil {
		return manifest, err
	} else if live {
		return manifest, fmt.Errorf("tmux session already exists: %s", manifest.Session)
	}
	logDirectory := filepath.Dir(manifest.LogPath)
	if err := mkdirBelow(r.StateRoot, logDirectory, 0o700); err != nil {
		return manifest, fmt.Errorf("create attempt state directory: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(manifest.Worktree), 0o770); err != nil {
		return manifest, err
	}
	if _, err := r.run(ctx, r.git(), []string{"clone", "--no-local", "--no-checkout", r.Source, manifest.Worktree}, "", nil, nil); err != nil {
		return failedEffect(manifest, "clone", err)
	}
	git := func(args ...string) error {
		_, err := r.run(ctx, r.git(), append([]string{"-C", manifest.Worktree}, args...), "", nil, nil)
		return err
	}
	for _, step := range []struct {
		name string
		args []string
	}{
		{name: "checkout base", args: []string{"checkout", "--detach", request.Attempt.BaseSHA}},
		{name: "create branch", args: []string{"switch", "-c", manifest.Branch}},
		{name: "remove remote", args: []string{"remote", "remove", "origin"}},
		{name: "disable credentials", args: []string{"config", "--local", "credential.helper", ""}},
	} {
		if err := git(step.args...); err != nil {
			return failedEffect(manifest, step.name, err)
		}
	}
	return manifest, nil
}

func (r *Runtime) startEffect(ctx context.Context, request EffectRequest, authorize func(context.Context, EffectRequest) error) (Manifest, error) {
	manifest := request.Manifest
	if authorize == nil {
		return manifest, errors.New("implementation Start lacks owner authorization")
	}
	if !request.Eligible {
		return cancelledEffect(manifest, "attempt became ineligible before launch"), errors.New("attempt became ineligible before launch")
	}
	if strings.TrimSpace(r.Helper) == "" {
		return manifest, errors.New("bound attempt helper is required")
	}
	if err := r.verifyEffectWorker(ctx); err != nil {
		return manifest, err
	}
	env := effectEnvironment(request)
	candidate := manifest
	candidate.LaunchID = request.GateNonce
	if candidate.LaunchID == "" {
		candidate.LaunchID = request.Identity.EffectID // Old intents remain quarantined by the owner.
	}
	live, err := r.session(ctx, manifest.Session)
	if err != nil {
		return manifest, err
	}
	if live {
		if _, _, err := r.observeBound(ctx, candidate); err != nil {
			return manifest, fmt.Errorf("pending implementation launch identity changed: %w", err)
		}
	} else if _, err := ReadImplementationBinding(candidate); err == nil {
		return manifest, errors.New("pending implementation launch remains bound; original pane and worker absence is unproved")
	} else if !errors.Is(err, os.ErrNotExist) {
		return manifest, fmt.Errorf("check pending implementation launch identity: %w", err)
	}
	if manifest.Interactive {
		result, err := os.OpenFile(ResultPath(manifest.Worktree), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			if !live || !errors.Is(err, os.ErrExist) {
				return failedEffect(manifest, "prepare worker result", err)
			}
			info, statErr := os.Lstat(ResultPath(manifest.Worktree))
			if statErr != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || info.Size() > WorkerResultMaxBytes {
				return failedEffect(manifest, "prepare worker result", errors.Join(err, statErr))
			}
		} else if err := result.Close(); err != nil {
			return failedEffect(manifest, "prepare worker result", err)
		}
		env = append(env, WorkerResultEnvironment+"="+ResultPath(manifest.Worktree))
	}
	command := slices.Clone(request.Attempt.Command)
	buffer := "as-start-" + candidate.LaunchID
	if request.Attempt.Interactive {
		command = append(command, request.Attempt.Context)
	} else if request.Attempt.Context != "" {
		command = BoundPromptCommand(r.Helper, r.tmux(), buffer, ResultPath(manifest.Worktree), candidate, command)
	}
	if request.Attempt.Interactive || request.Attempt.Context == "" {
		command = BoundPaneExitStatusCommand(r.Helper, r.tmux(), candidate, command)
	}
	if !live {
		if err := r.startSession(ctx, candidate, env, candidate.LaunchID, command); err != nil {
			return manifest, fmt.Errorf("launch tmux remains ambiguous: %w", err)
		}
	}
	manifest.LaunchID = candidate.LaunchID
	if request.Attempt.Context != "" && !request.Attempt.Interactive {
		binding, pane, err := r.observeBound(ctx, manifest)
		if err != nil {
			return manifest, fmt.Errorf("bind agent context: %w", err)
		}
		load, err := TmuxCommandString([]string{"load-buffer", "-b", buffer, "-"})
		if err != nil {
			return manifest, fmt.Errorf("bind agent context: %w", err)
		}
		if _, err := r.guardedBoundResult(ctx, binding, pane, load, strings.NewReader(request.Attempt.Context)); err != nil {
			return manifest, fmt.Errorf("load agent context: %w", err)
		}
	}
	if err := authorize(ctx, request); err != nil {
		return manifest, fmt.Errorf("authorize agent release: %w", err)
	}
	binding, _, err := r.observeBound(ctx, manifest)
	if err != nil {
		return manifest, err
	}
	if err := WriteImplementationPermit(manifest, binding); err != nil {
		return manifest, err
	}
	if err := ctx.Err(); err != nil {
		return manifest, err
	}
	if err := r.launchAgent(ctx, manifest); err != nil {
		return manifest, fmt.Errorf("start agent remains ambiguous: %w", err)
	}
	manifest.State, manifest.Diagnostic, manifest.UpdatedAt = "running", "", time.Now().UTC()
	return manifest, nil
}

func (r *Runtime) monitorEffect(ctx context.Context, request EffectRequest) (Manifest, error) {
	manifest := request.Manifest
	if !request.Eligible {
		return manifest, errors.New("ineligible attempt requires a generation-invalidating stop effect")
	}
	if manifest.Version != boundManifestVersion {
		live, err := r.session(ctx, manifest.Session)
		if err != nil {
			return manifest, err
		}
		if !live {
			return manifest, nil // Do not infer completion from an absent legacy pane.
		}
		return manifest, errors.New("legacy implementation pane has no durable observation identity")
	}
	if err := r.verifyEffectWorker(ctx); err != nil {
		return manifest, err
	}
	var result Result
	var runErr error
	result, runErr = r.observeBoundCommand(ctx, manifest, func(pane ImplementationPane) []string {
		return []string{"display-message", "-p", "-t", pane.PaneID, PaneStatusFormat}
	})
	if runErr != nil {
		if err := ctx.Err(); err != nil {
			return manifest, err
		}
		return manifest, fmt.Errorf("observe tmux session: %w", runErr)
	}
	pane, err := ParsePaneStatus(result.Output)
	if err != nil {
		return manifest, fmt.Errorf("observe tmux session: %w", err)
	}
	if !pane.Dead || !pane.Ready {
		return manifest, nil
	}
	env := effectEnvironment(request)
	var capture Result
	var captureErr error
	capture, captureErr = r.observeBoundCommand(ctx, manifest, func(pane ImplementationPane) []string {
		return []string{"capture-pane", "-p", "-S", "-", "-t", pane.PaneID}
	})
	if captureErr != nil {
		captureErr = errors.New(internalgithub.RedactEnvironment(captureErr.Error(), env))
	}
	var logErr error
	if captureErr == nil {
		logErr = os.WriteFile(manifest.LogPath, []byte(internalgithub.RedactEnvironment(capture.Output, env)), 0o600)
	}
	if captureErr != nil || logErr != nil {
		cause := errors.Join(captureErr, logErr)
		manifest.State, manifest.Diagnostic = "failed", "agent exited; output was not preserved: "+diagnostic(cause)
		manifest.UpdatedAt = time.Now().UTC()
		return manifest, cause
	}
	if pane.Signal != "" {
		manifest.State, manifest.Diagnostic = "failed", fmt.Sprintf("agent terminated by signal %s; output preserved in %s", pane.Signal, manifest.LogPath)
	} else if pane.ExitStatus != 0 {
		manifest.State, manifest.Diagnostic = "failed", fmt.Sprintf("agent exited with status %d; output preserved in %s", pane.ExitStatus, manifest.LogPath)
	} else if manifest.Interactive {
		info, err := os.Lstat(ResultPath(manifest.Worktree))
		if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 || info.Size() == 0 || info.Size() > WorkerResultMaxBytes {
			manifest.State, manifest.Diagnostic = "failed", "agent exited without a valid private result artifact"
		} else {
			manifest.State, manifest.Diagnostic = "completed", ""
		}
	} else {
		manifest.State, manifest.Diagnostic = "completed", ""
	}
	manifest.UpdatedAt = time.Now().UTC()
	return manifest, nil
}

func (r *Runtime) stopEffect(ctx context.Context, request EffectRequest) (Manifest, error) {
	manifest := request.Manifest
	if err := r.verifyEffectWorker(ctx); err != nil {
		return manifest, err
	}
	if err := r.stop(ctx, manifest); err != nil {
		return manifest, err
	}
	return cancelledEffect(manifest, request.Reason), nil
}

func (r *Runtime) handoffEffect(ctx context.Context, request EffectRequest, authorize func(context.Context, EffectRequest) error) (Manifest, error) {
	manifest := request.Manifest
	if authorize == nil {
		return manifest, errors.New("implementation Handoff lacks owner authorization")
	}
	if !request.Eligible {
		return manifest, errors.New("attempt is no longer eligible")
	}
	if err := r.verifyEffectWorker(ctx); err != nil {
		return manifest, err
	}
	if manifest.State != "completed" && manifest.State != "running" {
		return manifest, fmt.Errorf("cannot resume handoff from %s attempt", manifest.State)
	}
	if manifest.State == "completed" {
		if strings.TrimSpace(r.Source) == "" {
			return manifest, errors.New("handoff source bundle is required")
		}
		if _, err := r.run(ctx, r.git(), []string{"-C", manifest.Worktree, "fetch", "--no-tags", r.Source, "+refs/heads/*:refs/remotes/agent-symphony/*"}, "", nil, nil); err != nil {
			return manifest, fmt.Errorf("refresh handoff source refs: %w", err)
		}
	}
	live, err := r.session(ctx, manifest.Session)
	if err != nil {
		return manifest, err
	}
	candidate := manifest
	candidate.LaunchToken, candidate.LaunchID = request.CandidateLaunchToken, request.Identity.EffectID
	newLaunch := false
	if live && manifest.Version == boundManifestVersion {
		if _, err := r.observeBoundCommand(ctx, manifest, func(pane ImplementationPane) []string {
			return []string{"display-message", "-p", "-t", pane.PaneID, "#{pane_dead}"}
		}); err != nil {
			if _, _, candidateErr := r.observeBound(ctx, candidate); candidateErr != nil {
				return manifest, errors.Join(err, candidateErr)
			}
			newLaunch = true
		}
	}
	if !live {
		env := effectEnvironment(request)
		if r.Helper == "" {
			return manifest, errors.New("bound handoff shell helper is required")
		}
		command := BoundPaneExitStatusCommand(r.Helper, r.tmux(), candidate, []string{"/bin/sh"})
		if err := r.startSession(ctx, candidate, env, candidate.LaunchID, command); err != nil {
			return manifest, err
		}
		newLaunch = true
	}
	if newLaunch {
		if err := authorize(ctx, request); err != nil {
			return manifest, err
		}
		binding, _, err := r.observeBound(ctx, candidate)
		if err != nil {
			return manifest, err
		}
		if err := WriteImplementationPermit(candidate, binding); err != nil {
			return manifest, err
		}
		if err := r.launchAgent(ctx, candidate); err != nil {
			return manifest, err
		}
		manifest.LaunchToken, manifest.LaunchID = candidate.LaunchToken, candidate.LaunchID
	}
	manifest.State, manifest.Diagnostic, manifest.UpdatedAt = "running", "", time.Now().UTC()
	return manifest, nil
}

func (r *Runtime) reviewEffect(request EffectRequest) (Manifest, error) {
	return ReviewEffectResult(r.Root, r.StateRoot, request.Manifest, request.Review)
}

// ReviewEffectResult constructs and validates the pure manifest transition for
// an owner-approved review result.
func ReviewEffectResult(root, stateRoot string, manifest Manifest, review ReviewTransition) (Manifest, error) {
	if manifest.ReviewHandoffQueued && review.State == "findings-queued" {
		if !review.HandoffQueued || manifest.ReviewHead != review.Head || !slices.Equal(manifest.ReviewFindings, review.Findings) || manifest.ReviewHandoffAck && !review.HandoffAcknowledged || manifest.ReviewMode != review.Mode || manifest.ReviewTarget != review.Target || manifest.ReviewBase != review.Base || manifest.ReviewSnapshot != review.Snapshot || manifest.ReviewSession != review.Session {
			return Manifest{}, errors.New("queued review handoff is immutable")
		}
	}
	manifest.ReviewState, manifest.ReviewMode, manifest.ReviewTarget = review.State, review.Mode, review.Target
	manifest.ReviewBase, manifest.ReviewHead, manifest.ReviewSnapshot, manifest.ReviewSession = review.Base, review.Head, review.Snapshot, review.Session
	manifest.ReviewFindings = slices.Clone(review.Findings)
	manifest.ReviewHandoffQueued, manifest.ReviewHandoffAck = review.HandoffQueued, review.HandoffAcknowledged
	if review.State != "findings-queued" {
		manifest.ReviewFindings, manifest.ReviewHandoffQueued, manifest.ReviewHandoffAck = nil, false, false
	}
	if review.HandoffAcknowledged && manifest.State == "completed" {
		manifest.State, manifest.Diagnostic = "running", ""
	}
	manifest.UpdatedAt = time.Now().UTC()
	if err := ValidateManifest(root, stateRoot, manifest); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

func (r *Runtime) verifyPrepared(ctx context.Context, manifest Manifest) error {
	if err := r.verifyEffectWorker(ctx); err != nil {
		return err
	}
	info, err := os.Lstat(manifest.Worktree)
	if errors.Is(err, os.ErrNotExist) {
		return ErrWorktreeMissing
	}
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return ErrWorktreeUnsafe
	}
	branch, err := r.run(ctx, r.git(), []string{"-C", manifest.Worktree, "branch", "--show-current"}, "", nil, nil)
	if err != nil || strings.TrimSpace(branch.Output) != manifest.Branch {
		return errors.New("worktree branch does not match manifest")
	}
	head, err := r.run(ctx, r.git(), []string{"-C", manifest.Worktree, "rev-parse", "HEAD"}, "", nil, nil)
	if err != nil || !strings.EqualFold(strings.TrimSpace(head.Output), manifest.BaseSHA) {
		return errors.New("worktree HEAD does not match prepared base")
	}
	remotes, err := r.run(ctx, r.git(), []string{"-C", manifest.Worktree, "remote"}, "", nil, nil)
	if err != nil || strings.TrimSpace(remotes.Output) != "" {
		return errors.New("prepared worktree retained a remote")
	}
	credentials, err := r.run(ctx, r.git(), []string{"-C", manifest.Worktree, "config", "--local", "--get", "credential.helper"}, "", nil, nil)
	if err != nil || strings.TrimSpace(credentials.Output) != "" {
		return errors.New("prepared worktree retained credential helpers")
	}
	if live, err := r.session(ctx, manifest.Session); err != nil {
		return err
	} else if live {
		return errors.New("prepare effect unexpectedly launched a session")
	}
	if _, err := os.Lstat(ResultPath(manifest.Worktree)); err == nil {
		return errors.New("prepare effect unexpectedly created a result")
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	info, err = os.Lstat(filepath.Dir(manifest.LogPath))
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o077 != 0 {
		return errors.New("prepared attempt state directory is unsafe or missing")
	}
	if _, err := os.Lstat(filepath.Join(filepath.Dir(manifest.LogPath), "manifest.json")); err == nil {
		return errors.New("prepare effect unexpectedly wrote an authoritative manifest")
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func definitiveFailedEffect(action EffectAction, manifest Manifest) bool {
	return manifest.State == "failed" && slices.Contains([]EffectAction{EffectPrepare, EffectStart, EffectMonitor}, action)
}

func (r *Runtime) verifyResourcesGone(ctx context.Context, manifest Manifest) error {
	if err := r.verifyEffectWorker(ctx); err != nil {
		return err
	}
	live, err := r.session(ctx, manifest.Session)
	if err != nil {
		return err
	}
	if live {
		return ErrRuntimeResourcesRemain
	}
	for _, path := range []string{manifest.Worktree, ResultPath(manifest.Worktree), manifest.ReviewSnapshot} {
		if path == "" {
			continue
		}
		if _, err := os.Lstat(path); err == nil {
			return ErrRuntimeResourcesRemain
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func (r *Runtime) verifyEffectWorker(ctx context.Context) error {
	if r.VerifyWorker == nil {
		return errors.New("worker identity verification hook is required")
	}
	if err := r.VerifyWorker(ctx); err != nil {
		return fmt.Errorf("verify worker identity: %w", err)
	}
	return nil
}

func failedEffect(manifest Manifest, stage string, cause error) (Manifest, error) {
	manifest.State, manifest.Diagnostic, manifest.UpdatedAt = "failed", stage+": "+diagnostic(cause), time.Now().UTC()
	return manifest, fmt.Errorf("%s: %w", stage, cause)
}

func cancelledEffect(manifest Manifest, reason string) Manifest {
	manifest.State, manifest.Diagnostic, manifest.UpdatedAt = "cancelled", reason, time.Now().UTC()
	return manifest
}
