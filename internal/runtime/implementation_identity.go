package runtime

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

// ImplementationPaneFormat includes the process-independent pane identity and
// the live process PID. The launch token is a pane option, not a session name.
const ImplementationPaneFormat = "#{session_name}|#{session_id}|#{pane_id}|#{pane_pid}|#{pid}|#{start_time}|#{pane_start_path}|#{@agent-symphony-launch-token}|#{pane_start_command}"

// Launch commands include the bounded issue context. Keep every durable
// identity record bounded while allowing the full worker contract to be
// re-read after authorization and restart.
const implementationIdentityMaxBytes = 128 << 10

// Inventory exposes only the server generation and stable pane ID. A missing
// pane target can silently fall back to a different tmux pane; list-panes -a
// is the read-only exact-absence check while the original server is live.
const ImplementationInventoryFormat = "#{pid}|#{start_time}|#{pane_id}"

// ImplementationOriginalServerGone is an absence-only fallback when the
// original tmux socket cannot answer an inventory query. A live, inaccessible,
// or reused numeric PID is ambiguous and never authorizes a signal.
func ImplementationOriginalServerGone(binding ImplementationLaunchBinding) bool {
	return binding.ServerPID > 1 && errors.Is(syscall.Kill(binding.ServerPID, 0), syscall.ESRCH)
}

func ImplementationPaneAbsentFromInventory(output string, binding ImplementationLaunchBinding) (bool, error) {
	rows := strings.Split(strings.TrimSpace(output), "\n")
	if len(rows) == 0 || rows[0] == "" {
		return false, errors.New("implementation pane inventory is unavailable")
	}
	found := false
	for _, row := range rows {
		fields := strings.Split(row, "|")
		if len(fields) != 3 || !tmuxID(fields[2], '%') {
			return false, errors.New("implementation pane inventory is malformed")
		}
		serverPID, err := strconv.Atoi(fields[0])
		if err != nil || serverPID != binding.ServerPID {
			return false, errors.New("implementation tmux server changed during absence proof")
		}
		serverStart, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil || serverStart != binding.ServerStart {
			return false, errors.New("implementation tmux server changed during absence proof")
		}
		found = found || fields[2] == binding.PaneID
	}
	return !found, nil
}

const ImplementationGuardMismatch = "implementation-guard-mismatch"

func ImplementationGateChannel(effectID string) string { return "implementation-" + effectID }

type ImplementationPane struct {
	SessionName string
	SessionID   string
	PaneID      string
	PanePID     int
	ServerPID   int
	ServerStart uint64
	StartPath   string
	Token       string
	Command     string
}

func ParseImplementationPane(output string) (ImplementationPane, error) {
	parts := strings.SplitN(strings.TrimSpace(output), "|", 9)
	if len(parts) != 9 || parts[0] == "" || !tmuxID(parts[1], '$') || !tmuxID(parts[2], '%') || parts[6] == "" {
		return ImplementationPane{}, errors.New("implementation pane identity is unavailable")
	}
	panePID, err := strconv.Atoi(parts[3])
	if err != nil || panePID < 2 {
		return ImplementationPane{}, errors.New("implementation pane PID is unavailable")
	}
	serverPID, err := strconv.Atoi(parts[4])
	if err != nil || serverPID < 2 {
		return ImplementationPane{}, errors.New("implementation server PID is unavailable")
	}
	serverStart, err := strconv.ParseUint(parts[5], 10, 64)
	if err != nil || serverStart == 0 {
		return ImplementationPane{}, errors.New("implementation server start is unavailable")
	}
	if token, err := hex.DecodeString(parts[7]); parts[7] != "" && (err != nil || len(token) != 16) {
		return ImplementationPane{}, errors.New("implementation launch token is invalid")
	}
	return ImplementationPane{parts[0], parts[1], parts[2], panePID, serverPID, serverStart, parts[6], parts[7], parts[8]}, nil
}

func tmuxID(value string, prefix byte) bool {
	if len(value) < 2 || value[0] != prefix {
		return false
	}
	_, err := strconv.ParseUint(value[1:], 10, 64)
	return err == nil
}

// ImplementationLaunchBinding is durable external process evidence. Runtime
// state transitions still commit only through the owner.
type ImplementationLaunchBinding struct {
	Version     int    `json:"version"`
	Role        string `json:"role"`
	Token       string `json:"token"`
	EffectID    string `json:"effect_id"`
	ServerPID   int    `json:"server_pid"`
	ServerStart uint64 `json:"server_start"`
	SessionName string `json:"session_name"`
	SessionID   string `json:"session_id"`
	PaneID      string `json:"pane_id"`
	PanePID     int    `json:"pane_pid"`
	StartPath   string `json:"start_path"`
	Command     string `json:"command"`
}

func ImplementationBindingPath(manifest Manifest, launchID string) string {
	return filepath.Join(filepath.Dir(manifest.LogPath), "implementation-launch-"+launchID+".json")
}

func implementationReleasePath(manifest Manifest) string {
	return filepath.Join(filepath.Dir(manifest.LogPath), "implementation-release-"+manifest.LaunchID+".json")
}

func implementationPermitPath(manifest Manifest) string {
	return filepath.Join(filepath.Dir(manifest.LogPath), "implementation-permit-"+manifest.LaunchID+".json")
}

func implementationGateEnteredPath(manifest Manifest) string {
	return filepath.Join(filepath.Dir(manifest.LogPath), "implementation-entered-"+manifest.LaunchID+".json")
}

func WriteImplementationGateEntered(manifest Manifest, binding ImplementationLaunchBinding) error {
	if binding.EffectID != manifest.LaunchID || binding.Token != manifest.LaunchToken || !implementationPermitMatches(manifest, binding) || !implementationReleaseMatches(manifest, binding) {
		return errors.New("implementation gate entry identity is invalid")
	}
	body, err := json.Marshal(binding)
	if err != nil {
		return err
	}
	if len(body)+1 > implementationIdentityMaxBytes {
		return errors.New("implementation launch binding exceeds limit")
	}
	return writeImmutableImplementationGroup(implementationGateEnteredPath(manifest), body)
}

func ImplementationGateEntered(manifest Manifest, binding ImplementationLaunchBinding) (bool, error) {
	body, err := readImmutableIdentity(implementationGateEnteredPath(manifest))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	want, err := json.Marshal(binding)
	if err != nil || !bytes.Equal(body, want) {
		return false, errors.New("implementation gate entry conflicts with durable identity")
	}
	return true, nil
}

// The state owner must durably admit this exact candidate before its executor
// writes the permit. A direct Runtime launch has no permit and cannot exec.
func WriteImplementationPermit(manifest Manifest, binding ImplementationLaunchBinding) error {
	if binding.EffectID != manifest.LaunchID || binding.Token != manifest.LaunchToken {
		return errors.New("implementation permit identity is invalid")
	}
	body, err := json.Marshal(binding)
	if err != nil {
		return err
	}
	return writeImmutableImplementationGroup(implementationPermitPath(manifest), body)
}

func implementationPermitMatches(manifest Manifest, binding ImplementationLaunchBinding) bool {
	actual, err := readImmutableIdentity(implementationPermitPath(manifest))
	if err != nil {
		return false
	}
	want, err := json.Marshal(binding)
	return err == nil && bytes.Equal(actual, want)
}

// The release proof is written only after owner authorization and before
// unlocking the parked gate. It makes an already-unlocked gate replayable.
func WriteImplementationRelease(manifest Manifest, binding ImplementationLaunchBinding) error {
	if binding.EffectID != manifest.LaunchID || binding.Token != manifest.LaunchToken {
		return errors.New("implementation release identity is invalid")
	}
	body, err := json.Marshal(binding)
	if err != nil {
		return err
	}
	path := implementationReleasePath(manifest)
	if existing, err := readImmutableIdentity(path); err == nil {
		if !bytes.Equal(existing, body) {
			return errors.New("implementation release proof conflicts with durable identity")
		}
		return syncIdentityDir(path)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".implementation-release-*")
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
		existing, readErr := readImmutableIdentity(path)
		if readErr != nil || !bytes.Equal(existing, body) {
			return errors.New("implementation release proof conflicts with durable identity")
		}
		return syncIdentityDir(path)
	} else if err != nil {
		return err
	}
	return syncIdentityDir(path)
}

func syncIdentityDir(path string) error {
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func implementationReleaseMatches(manifest Manifest, binding ImplementationLaunchBinding) bool {
	actual, err := readImmutableIdentity(implementationReleasePath(manifest))
	if err != nil {
		return false
	}
	want, err := json.Marshal(binding)
	return err == nil && bytes.Equal(actual, want)
}

// ImplementationReleaseMatches proves this exact gate was previously
// authorized for release; it is required to accept an already-unlocked replay.
func ImplementationReleaseMatches(manifest Manifest, binding ImplementationLaunchBinding) bool {
	return implementationReleaseMatches(manifest, binding)
}

func readImmutableIdentity(path string) ([]byte, error) {
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	listed, listErr := os.Lstat(path)
	opened, statErr := file.Stat()
	body, readErr := io.ReadAll(io.LimitReader(file, implementationIdentityMaxBytes+1))
	closeErr := file.Close()
	if listErr != nil || statErr != nil || readErr != nil || closeErr != nil || !os.SameFile(listed, opened) || !opened.Mode().IsRegular() || opened.Mode().Perm() != 0o600 || len(body) > implementationIdentityMaxBytes {
		return nil, errors.New("implementation identity proof is unsafe")
	}
	return body, nil
}

func validImplementationRole(role string) bool {
	return role == "capture" || role == "interactive" || role == "unknown"
}

func BindImplementationPane(manifest Manifest, effectID, role string, pane ImplementationPane) (ImplementationLaunchBinding, error) {
	if !ValidManifestVersion(manifest) || manifest.Version != boundManifestVersion || !tmuxLaunchID(effectID) || !validImplementationRole(role) || pane.SessionName != manifest.Session || pane.StartPath != manifest.Worktree || pane.Token != manifest.LaunchToken || pane.Command == "" {
		return ImplementationLaunchBinding{}, errors.New("implementation pane does not match owner-committed launch")
	}
	return ImplementationLaunchBinding{Version: 1, Role: role, Token: pane.Token, EffectID: effectID, ServerPID: pane.ServerPID, ServerStart: pane.ServerStart, SessionName: pane.SessionName, SessionID: pane.SessionID, PaneID: pane.PaneID, PanePID: pane.PanePID, StartPath: pane.StartPath, Command: pane.Command}, nil
}

func (binding ImplementationLaunchBinding) Matches(manifest Manifest, pane ImplementationPane) bool {
	return binding.Version == 1 && manifest.Version == boundManifestVersion && binding.Token == manifest.LaunchToken && binding.SessionName == manifest.Session && binding.StartPath == manifest.Worktree &&
		pane.Token == binding.Token && pane.ServerPID == binding.ServerPID && pane.ServerStart == binding.ServerStart && pane.SessionName == binding.SessionName && pane.SessionID == binding.SessionID && pane.PaneID == binding.PaneID && pane.PanePID == binding.PanePID && pane.StartPath == binding.StartPath && pane.Command == binding.Command
}

func WriteImplementationBinding(manifest Manifest, binding ImplementationLaunchBinding) error {
	if !ValidImplementationBinding(manifest, binding, binding.EffectID) {
		return errors.New("implementation launch binding is invalid")
	}
	path := ImplementationBindingPath(manifest, binding.EffectID)
	body, err := json.Marshal(binding)
	if err != nil {
		return err
	}
	if existing, err := readImplementationBinding(manifest, binding.EffectID); err == nil {
		if existing != binding {
			return errors.New("implementation launch binding conflicts with durable identity")
		}
		return syncIdentityDir(path)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".implementation-launch-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(append(body, '\n')); err != nil {
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
		existing, readErr := readImplementationBinding(manifest, binding.EffectID)
		if readErr != nil || existing != binding {
			return errors.New("implementation launch binding conflicts with durable identity")
		}
		return syncIdentityDir(path)
	} else if err != nil {
		return err
	}
	return syncIdentityDir(path)
}

func ReadImplementationBinding(manifest Manifest) (ImplementationLaunchBinding, error) {
	return readImplementationBinding(manifest, manifest.LaunchID)
}

// ValidImplementationBinding checks the durable binding's complete identity,
// including fields needed after its original file has been removed.
func ValidImplementationBinding(manifest Manifest, binding ImplementationLaunchBinding, launchID string) bool {
	return binding.Version == 1 && validImplementationRole(binding.Role) && binding.Token == manifest.LaunchToken && binding.SessionName == manifest.Session && binding.StartPath == manifest.Worktree && binding.EffectID == launchID && tmuxLaunchID(launchID) && binding.Command != "" && binding.PanePID >= 2 && binding.ServerPID >= 2 && binding.ServerStart != 0 && tmuxID(binding.SessionID, '$') && tmuxID(binding.PaneID, '%')
}

func readImplementationBinding(manifest Manifest, launchID string) (ImplementationLaunchBinding, error) {
	if !tmuxLaunchID(launchID) {
		return ImplementationLaunchBinding{}, errors.New("implementation launch ID is invalid")
	}
	path := ImplementationBindingPath(manifest, launchID)
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return ImplementationLaunchBinding{}, err
	}
	listed, listErr := os.Lstat(path)
	opened, statErr := file.Stat()
	body, readErr := io.ReadAll(io.LimitReader(file, implementationIdentityMaxBytes+1))
	closeErr := file.Close()
	if listErr != nil || statErr != nil || readErr != nil || closeErr != nil || !os.SameFile(listed, opened) || !opened.Mode().IsRegular() || opened.Mode().Perm() != 0o600 || len(body) > implementationIdentityMaxBytes {
		return ImplementationLaunchBinding{}, errors.New("implementation launch binding is unsafe")
	}
	var binding ImplementationLaunchBinding
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&binding) != nil || decoder.Decode(&struct{}{}) != io.EOF || !ValidImplementationBinding(manifest, binding, launchID) {
		return ImplementationLaunchBinding{}, fmt.Errorf("implementation launch binding is invalid")
	}
	return binding, nil
}

func tmuxLaunchID(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 16
}

func ImplementationGuardCondition(binding ImplementationLaunchBinding, pane ImplementationPane) (string, error) {
	if pane.ServerPID != binding.ServerPID || pane.ServerStart != binding.ServerStart || pane.SessionName != binding.SessionName || pane.SessionID != binding.SessionID || pane.PaneID != binding.PaneID || pane.Token != binding.Token || pane.PanePID != binding.PanePID || pane.Command != binding.Command {
		return "", errors.New("implementation pane identity changed")
	}
	return fmt.Sprintf("#{&&:#{==:#{pid},%d},#{&&:#{==:#{start_time},%d},#{&&:#{==:#{session_name},%s},#{&&:#{==:#{session_id},%s},#{&&:#{==:#{pane_id},%s},#{&&:#{==:#{pane_pid},%d},#{==:#{@agent-symphony-launch-token},%s}}}}}}}", binding.ServerPID, binding.ServerStart, binding.SessionName, binding.SessionID, binding.PaneID, pane.PanePID, binding.Token), nil
}

// GuardedImplementationArgs queues identity validation and one mutation in
// the same tmux server. A mismatch emits a marker and never runs the mutation.
func GuardedImplementationArgs(binding ImplementationLaunchBinding, pane ImplementationPane, command string) ([]string, error) {
	condition, err := ImplementationGuardCondition(binding, pane)
	if err != nil || command == "" {
		return nil, errors.New("implementation pane guard is invalid")
	}
	return []string{"if-shell", "-F", "-t", pane.PaneID, condition, command, "display-message -p " + ImplementationGuardMismatch}, nil
}

// TmuxCommandString quotes argv for a nested tmux command-list argument.
func TmuxCommandString(args []string) (string, error) {
	if len(args) == 0 {
		return "", errors.New("empty nested tmux command")
	}
	parts := make([]string, len(args))
	for index, arg := range args {
		if strings.ContainsRune(arg, 0) {
			return "", errors.New("invalid nested tmux argument")
		}
		parts[index] = "'" + strings.ReplaceAll(arg, "'", "'\\''") + "'"
	}
	return strings.Join(parts, " "), nil
}
