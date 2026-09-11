package main

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"syscall"
)

const maxReconciliationMarkerBytes = 1 << 20

type reconciliationMarkerIdentity struct {
	Action            reconciliationEffectAction `json:"action"`
	Repository        string                     `json:"repository"`
	Issue             int                        `json:"issue"`
	Attempt           int                        `json:"attempt"`
	Epoch             uint64                     `json:"epoch"`
	SourceRevision    uint64                     `json:"source_revision"`
	IssueGeneration   uint64                     `json:"issue_generation"`
	AttemptGeneration uint64                     `json:"attempt_generation"`
	EffectID          string                     `json:"effect_id"`
	RequestDigest     string                     `json:"request_digest"`
}

type reconciliationEffectMarker struct {
	Version  int                          `json:"version"`
	Identity reconciliationMarkerIdentity `json:"identity"`
	Result   reconciliationEffectResult   `json:"result"`
}

func writeReconciliationEffectMarker(stateRoot string, identity stateResultIdentity, request reconciliationEffectRequest, result reconciliationEffectResult) error {
	marker, err := newReconciliationEffectMarker(identity, request, result)
	if err != nil {
		return err
	}
	directory, err := reconciliationMarkerDirectory(stateRoot, true)
	if err != nil {
		return err
	}
	path, err := reconciliationMarkerPath(directory, identity.EffectID)
	if err != nil {
		return err
	}
	body, err := json.Marshal(marker)
	if err != nil || len(body)+1 > maxReconciliationMarkerBytes {
		return errors.New("reconciliation effect marker is too large")
	}
	temporary, err := os.CreateTemp(directory, ".effect-*")
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
	if err := os.Link(name, path); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return err
		}
		existing, readErr := readReconciliationEffectMarker(stateRoot, identity, request)
		if readErr != nil || existing == nil || !reflect.DeepEqual(*existing, result) {
			return errors.New("reconciliation effect marker is immutable")
		}
	}
	dir, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func readReconciliationEffectMarker(stateRoot string, identity stateResultIdentity, request reconciliationEffectRequest) (*reconciliationEffectResult, error) {
	directory, err := reconciliationMarkerDirectory(stateRoot, false)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	path, err := reconciliationMarkerPath(directory, identity.EffectID)
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
	if err != nil {
		return nil, err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !os.SameFile(listed, opened) || !opened.Mode().IsRegular() || opened.Mode()&os.ModeSymlink != 0 || opened.Mode().Perm() != 0o600 || !ownedByCurrentUser(opened) || opened.Size() < 2 || opened.Size() > maxReconciliationMarkerBytes {
		return nil, errors.New("reconciliation effect marker is unsafe")
	}
	body, err := io.ReadAll(io.LimitReader(file, maxReconciliationMarkerBytes+1))
	if err != nil {
		return nil, err
	}
	if len(body) > maxReconciliationMarkerBytes {
		return nil, errors.New("reconciliation effect marker is too large")
	}
	var marker reconciliationEffectMarker
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&marker) != nil || decoder.Decode(&struct{}{}) != io.EOF || marker.Version != 1 || marker.Identity != reconciliationMarkerIdentityFor(identity, request) || !validReconciliationEffectRequest(request.Repository, request) || !validReconciliationEffectResult(request, marker.Result) {
		return nil, errors.New("reconciliation effect marker conflicts with its intent")
	}
	result := cloneReconciliationResult(marker.Result)
	return &result, nil
}

func newReconciliationEffectMarker(identity stateResultIdentity, request reconciliationEffectRequest, result reconciliationEffectResult) (reconciliationEffectMarker, error) {
	if identity.Epoch == 0 || identity.SourceRevision == 0 || identity.IssueGeneration == 0 || request.Attempt > 0 && identity.AttemptGeneration == 0 || identity.EffectID == "" || !validDigest(identity.RequestDigest) {
		return reconciliationEffectMarker{}, errors.New("reconciliation effect marker identity is invalid")
	}
	if identity.RequestDigest != reconciliationEffectDigest(request) || !validReconciliationEffectRequest(request.Repository, request) {
		return reconciliationEffectMarker{}, errors.New("reconciliation effect marker request conflicts with its intent")
	}
	if !validReconciliationEffectResult(request, result) {
		return reconciliationEffectMarker{}, errors.New("reconciliation effect marker result is invalid")
	}
	return reconciliationEffectMarker{Version: 1, Identity: reconciliationMarkerIdentityFor(identity, request), Result: cloneReconciliationResult(result)}, nil
}

func reconciliationMarkerIdentityFor(identity stateResultIdentity, request reconciliationEffectRequest) reconciliationMarkerIdentity {
	return reconciliationMarkerIdentity{Action: request.Action, Repository: request.Repository, Issue: request.Issue, Attempt: request.Attempt, Epoch: identity.Epoch, SourceRevision: identity.SourceRevision, IssueGeneration: identity.IssueGeneration, AttemptGeneration: identity.AttemptGeneration, EffectID: identity.EffectID, RequestDigest: identity.RequestDigest}
}

func reconciliationMarkerDirectory(stateRoot string, create bool) (string, error) {
	directory := filepath.Join(stateRoot, "reconciliation-effects")
	created := false
	if create {
		if err := os.Mkdir(directory, 0o700); err != nil {
			if !errors.Is(err, os.ErrExist) {
				return "", err
			}
		} else {
			created = true
		}
	}
	info, err := os.Lstat(directory)
	if err != nil {
		return "", err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o700 || !ownedByCurrentUser(info) {
		return "", errors.New("reconciliation effect marker directory is unsafe")
	}
	if created {
		if err := immutableDirSync(stateRoot); err != nil {
			return "", err
		}
	}
	return directory, nil
}

func reconciliationMarkerPath(directory, effectID string) (string, error) {
	decoded, err := hex.DecodeString(effectID)
	if err != nil || len(decoded) != 16 {
		return "", errors.New("reconciliation effect ID is invalid")
	}
	return filepath.Join(directory, effectID+".done"), nil
}

func removeReconciliationEffectMarker(stateRoot string, identity stateResultIdentity) error {
	directory, err := reconciliationMarkerDirectory(stateRoot, false)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	path, err := reconciliationMarkerPath(directory, identity.EffectID)
	if err != nil {
		return err
	}
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

func reclaimOrphanReconciliationMarkers(stateRoot string, pending map[string]bool) error {
	directory, err := reconciliationMarkerDirectory(stateRoot, false)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		return err
	}
	var orphans []string
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || entry.Type()&os.ModeSymlink != 0 || !strings.HasSuffix(name, ".done") {
			return errors.New("reconciliation effect marker directory contains an unsafe entry")
		}
		id := strings.TrimSuffix(name, ".done")
		path, err := reconciliationMarkerPath(directory, id)
		if err != nil {
			return err
		}
		listed, err := os.Lstat(path)
		if err != nil {
			return err
		}
		file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
		if err != nil {
			return err
		}
		opened, statErr := file.Stat()
		body, readErr := io.ReadAll(io.LimitReader(file, maxReconciliationMarkerBytes+1))
		closeErr := file.Close()
		if statErr != nil || readErr != nil || closeErr != nil || !os.SameFile(listed, opened) || !opened.Mode().IsRegular() || opened.Mode().Perm() != 0o600 || !ownedByCurrentUser(opened) || len(body) > maxReconciliationMarkerBytes {
			return errors.New("reconciliation effect marker is unsafe")
		}
		var marker reconciliationEffectMarker
		decoder := json.NewDecoder(strings.NewReader(string(body)))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&marker) != nil || decoder.Decode(&struct{}{}) != io.EOF || !validOrphanReconciliationMarker(marker, id) {
			return errors.New("reconciliation effect marker is invalid")
		}
		if !pending[id] {
			orphans = append(orphans, path)
		}
	}
	for _, path := range orphans {
		if err := os.Remove(path); err != nil {
			return err
		}
	}
	if len(orphans) == 0 {
		return nil
	}
	dir, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func validOrphanReconciliationMarker(marker reconciliationEffectMarker, id string) bool {
	identity := marker.Identity
	if marker.Version != 1 || identity.EffectID != id || identity.Repository == "" || identity.Issue < 1 || identity.Epoch == 0 || identity.SourceRevision == 0 || identity.IssueGeneration == 0 || !validDigest(identity.RequestDigest) || marker.Result.Action != identity.Action {
		return false
	}
	if !slices.Contains([]reconciliationEffectAction{reconciliationGitHubBind, reconciliationGitHubPublish, reconciliationGitHubIssueUpdate, reconciliationGitHubPRGovernance, reconciliationReviewer, reconciliationHandoffDeliver, reconciliationRetireCompleted, reconciliationMonitoringCheckIn}, identity.Action) {
		return false
	}
	if identity.Attempt == 0 && identity.AttemptGeneration != 0 || identity.Attempt > 0 && identity.AttemptGeneration == 0 {
		return false
	}
	return countTrue([]bool{marker.Result.GitHubBind != nil, marker.Result.GitHubPublish != nil, marker.Result.GitHubIssueUpdate != nil, marker.Result.GitHubPRGovernance != nil, marker.Result.Reviewer != nil, marker.Result.Handoff != nil, marker.Result.Retire != nil, marker.Result.CheckIn != nil}) == 1
}
