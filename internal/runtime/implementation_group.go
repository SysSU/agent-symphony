package runtime

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

// ImplementationGroupStart is external supervision evidence, not runtime
// state. The child remains behind a pipe gate until this record is durable.
type ImplementationGroupStart struct {
	Version   int                         `json:"version"`
	Binding   ImplementationLaunchBinding `json:"binding"`
	Role      string                      `json:"role"`
	GroupPID  int                         `json:"group_pid"`
	WorkerPID int                         `json:"worker_pid"`
}

func implementationGroupPath(manifest Manifest, role, phase string) string {
	return ImplementationBindingPath(manifest, manifest.LaunchID) + "." + role + "." + phase
}

func validImplementationGroupRecord(manifest Manifest, binding ImplementationLaunchBinding, record ImplementationGroupStart) bool {
	return record.Version == 1 && record.Binding == binding && binding.EffectID == manifest.LaunchID && binding.Token == manifest.LaunchToken && record.Role == binding.Role && (record.Role == "capture" || record.Role == "interactive") && record.GroupPID > 1 && record.WorkerPID > 1
}

func writeImmutableImplementationGroup(path string, body []byte) error {
	if prior, err := readImmutableIdentity(path); err == nil {
		if !bytes.Equal(prior, body) {
			return errors.New("implementation group proof conflicts with durable evidence")
		}
		return syncIdentityDir(path)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".implementation-group-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(body); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Link(tmp.Name(), path); errors.Is(err, os.ErrExist) {
		prior, readErr := readImmutableIdentity(path)
		if readErr != nil || !bytes.Equal(prior, body) {
			return errors.New("implementation group proof conflicts with durable evidence")
		}
	} else if err != nil {
		return err
	}
	return syncIdentityDir(path)
}

// WriteImplementationGroupStart is called with a live child handle, before
// its command gate is released. A conflicting record never gets replaced.
func WriteImplementationGroupStart(manifest Manifest, binding ImplementationLaunchBinding, role string, groupPID, workerPID int) (ImplementationGroupStart, error) {
	record := ImplementationGroupStart{Version: 1, Binding: binding, Role: role, GroupPID: groupPID, WorkerPID: workerPID}
	if !validImplementationGroupRecord(manifest, binding, record) {
		return ImplementationGroupStart{}, errors.New("implementation group start identity is invalid")
	}
	body, err := json.Marshal(record)
	if err != nil {
		return ImplementationGroupStart{}, err
	}
	if err := writeImmutableImplementationGroup(implementationGroupPath(manifest, role, "start"), body); err != nil {
		return ImplementationGroupStart{}, err
	}
	return record, nil
}

func readImplementationGroup(manifest Manifest, binding ImplementationLaunchBinding, role, phase string) (ImplementationGroupStart, error) {
	body, err := readImmutableIdentity(implementationGroupPath(manifest, role, phase))
	if err != nil {
		return ImplementationGroupStart{}, err
	}
	var record ImplementationGroupStart
	if json.Unmarshal(body, &record) != nil || !validImplementationGroupRecord(manifest, binding, record) || record.Role != role {
		return ImplementationGroupStart{}, errors.New("implementation group proof is invalid")
	}
	return record, nil
}

// WriteImplementationGroupDead records that the original group ended. It is
// not proof that a child which changed process groups has also ended.
func WriteImplementationGroupDead(manifest Manifest, binding ImplementationLaunchBinding, record ImplementationGroupStart) error {
	if !validImplementationGroupRecord(manifest, binding, record) {
		return errors.New("implementation group death identity is invalid")
	}
	terminated, err := implementationGroupTerminated(record.GroupPID)
	if err != nil || !terminated {
		return errors.New("implementation worker group termination is unproved")
	}
	body, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return writeImmutableImplementationGroup(implementationGroupPath(manifest, record.Role, "dead"), body)
}

// ImplementationGroupGone never signals a stored PID. Once a worker has
// started, neither ESRCH nor a dead-group record proves escaped descendants
// are gone, so physical cleanup must remain pending.
func ImplementationGroupGone(manifest Manifest, binding ImplementationLaunchBinding, role string) (bool, error) {
	otherRole := "interactive"
	if role == "interactive" {
		otherRole = "capture"
	}
	for _, phase := range []string{"start", "dead"} {
		if _, err := os.Lstat(implementationGroupPath(manifest, otherRole, phase)); err == nil {
			return false, errors.New("implementation worker group role conflicts with durable proof")
		} else if !errors.Is(err, os.ErrNotExist) {
			return false, err
		}
	}
	start, err := readImplementationGroup(manifest, binding, role, "start")
	if errors.Is(err, os.ErrNotExist) {
		// The release gate cannot execute the command before Start is durable.
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if dead, err := readImplementationGroup(manifest, binding, role, "dead"); err == nil {
		if dead != start {
			return false, errors.New("implementation group death proof conflicts with start")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	return false, errors.New("implementation worker descendants remain unproved")
}

// ImplementationWorkerGone accepts a missing group start only for helpers
// whose pipe gate cannot execute a worker before that record is durable.
func ImplementationWorkerGone(manifest Manifest, binding ImplementationLaunchBinding) (bool, error) {
	if binding.Role != "capture" && binding.Role != "interactive" {
		return false, errors.New("implementation worker group role is unavailable")
	}
	return ImplementationGroupGone(manifest, binding, binding.Role)
}
