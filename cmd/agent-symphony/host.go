package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	internalgithub "github.com/SysSU/agent-symphony/internal/github"
	"github.com/SysSU/agent-symphony/internal/orchestratoragent"
	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

const (
	workerUser             = "agent-symphony-worker"
	reviewerUser           = "agent-symphony-reviewer"
	attemptGroup           = "agent-symphony-attempt"
	snapshotGroup          = "agent-symphony-snapshot"
	orchestratorLaunchFile = "orchestrator-launch.json"
)

var (
	hostGOOS            = runtime.GOOS
	hostEUID            = os.Geteuid
	hostEGID            = os.Getegid
	hostExecutable      = os.Executable
	hostLookupUser      = user.Lookup
	hostLookupGroup     = user.LookupGroup
	hostCurrentUser     = user.Current
	hostGetwd           = os.Getwd
	hostOrchestratorRun = func(ctx context.Context, command agentruntime.Command) error {
		cmd := exec.CommandContext(ctx, command.Name, command.Args...)
		stdin := command.Stdin
		if stdin == nil {
			stdin = os.Stdin
		}
		cmd.Dir, cmd.Env, cmd.Stdin, cmd.Stdout, cmd.Stderr = command.Dir, command.Env, stdin, os.Stdout, os.Stderr
		return cmd.Run()
	}
	hostRun = func(name string, args ...string) error {
		out, err := exec.Command(name, args...).CombinedOutput()
		if err != nil {
			return fmt.Errorf("%s: %w: %s", name, err, strings.TrimSpace(string(out)))
		}
		return nil
	}
	hostOutput     = func(name string, args ...string) ([]byte, error) { return exec.Command(name, args...).Output() }
	hostExecRunner = (agentruntime.ExecRunner{}).Run
	hostProbe      = func(name string, args []string, input []byte) error {
		cmd := exec.Command(name, args...)
		cmd.Env, cmd.Stdin = minimalBoundaryEnvironment(), bytes.NewReader(input)
		if out, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("%w: %s", err, strings.TrimSpace(string(out)))
		}
		return nil
	}
	hostReviewResultOpen = syscall.Open
	hostRoot             = ""
)

func runHostTmux(ctx context.Context, args []string, stdin io.Reader) (agentruntime.Result, error) {
	return hostExecRunner(ctx, agentruntime.Command{Name: "tmux", Args: args, Dir: "/tmp", Stdin: stdin})
}

func nativeRoot(path string) string { return filepath.Join(hostRoot, path) }

// hostIsolationInstalled reports whether install-host has ever provisioned the
// advanced cross-UID boundary. Its absence is the sole signal that selects the
// zero-admin default (local) boundary instead: running install-host is itself
// the explicit opt-in into advanced mode, so there is exactly one source of
// truth and no separate config flag is needed.
func hostIsolationInstalled() bool {
	_, err := hostLookupUser(workerUser)
	return err == nil
}

func localAttemptRoot(stateRoot string) string  { return filepath.Join(stateRoot, "worktrees") }
func localSnapshotRoot(stateRoot string) string { return filepath.Join(stateRoot, "snapshots") }

// ensureLocalRoot creates path as a private mode-0700 directory owned by the
// current user when absent, or validates that an existing path is a
// non-symlink directory with no group/world access. Unlike ensureHostRoot,
// there is no separate identity to chown to: the coordinator and the local
// boundary run as the same OS user.
func ensureLocalRoot(path string) error {
	info, err := os.Lstat(path)
	if err == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
			return fmt.Errorf("%s has conflicting type or mode", path)
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	parent := filepath.Dir(path)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return err
	}
	tmp, err := os.MkdirTemp(parent, ".agent-symphony-local-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	if err := os.Chmod(tmp, 0o700); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		if _, statErr := os.Stat(path); statErr == nil {
			return ensureLocalRoot(path)
		}
		return err
	}
	return nil
}

// verifyLocalAccess is the local-mode counterpart to verifyHostAccess: it
// proves the provisioned root is a private, writable directory rather than
// running the cross-UID sudo allow/deny canary matrix, which has no meaning
// when the coordinator and the boundary share one OS identity.
func verifyLocalAccess(root string) error {
	if err := ensureLocalRoot(root); err != nil {
		return fmt.Errorf("provisioned local root: %w", err)
	}
	canary, err := os.MkdirTemp(root, ".doctor-local-")
	if err != nil {
		return fmt.Errorf("write provisioned local root: %w", err)
	}
	return os.RemoveAll(canary)
}

func installHost(coordinator string) error {
	if hostGOOS != "linux" && hostGOOS != "darwin" {
		return errors.New("host isolation supports macOS and Linux/WSL2 only")
	}
	if hostEUID() != 0 {
		return errors.New("install-host must run as root")
	}
	if strings.TrimSpace(coordinator) != coordinator || coordinator == "" || strings.ContainsAny(coordinator, "/:\n\r") {
		return errors.New("invalid coordinator user")
	}
	if _, err := hostLookupUser(coordinator); err != nil {
		return fmt.Errorf("coordinator user: %w", err)
	}
	binary, err := hostExecutable()
	if err != nil {
		return err
	}
	binary, err = filepath.EvalSymlinks(binary)
	if err != nil {
		return err
	}
	if err := validateInstalledBinary(binary); err != nil {
		return err
	}
	fd, err := syscall.Open(binary, syscall.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return errors.New("open immutable installed binary without following links")
	}
	defer syscall.Close(fd)
	opened := os.NewFile(uintptr(fd), binary)
	openedInfo, err := opened.Stat()
	if err != nil {
		return err
	}
	if listed, err := hostOutput("sudo", "-n", "-l", "-U", coordinator); err != nil || !installableSudoAuthority(listed, binary) {
		return errors.New("coordinator has unmanaged or broader sudo authority; remove it before installation")
	}
	if err := provisionIdentities(coordinator); err != nil {
		return err
	}
	if err := validateProvisionedIdentitySeparation(coordinator); err != nil {
		return err
	}
	base := "/var/lib/agent-symphony"
	if hostGOOS == "darwin" {
		base = "/var/db/agent-symphony"
	}
	for _, root := range []struct{ path, owner, group, mode string }{
		{base + "/attempts", workerUser, attemptGroup, "2770"},
		{base + "/snapshots", coordinator, snapshotGroup, "0750"},
	} {
		if err := ensureHostRoot(nativeRoot(root.path), root.owner, root.group, root.mode); err != nil {
			return err
		}
	}
	currentInfo, err := os.Lstat(binary)
	if err != nil || !os.SameFile(openedInfo, currentInfo) {
		return errors.New("installed binary changed during installation")
	}
	sudoersPath := nativeRoot("/etc/sudoers.d/agent-symphony")
	previousSudoers, previousErr := os.ReadFile(sudoersPath)
	installed, err := writeSudoers(coordinator, binary)
	if err != nil {
		return err
	}
	currentInfo, err = os.Lstat(binary)
	if err != nil || !os.SameFile(openedInfo, currentInfo) {
		_ = rollbackSudoers(sudoersPath, previousSudoers, !installed && previousErr == nil)
		return errors.New("installed binary changed during installation; sudo rules were not trusted")
	}
	return nil
}

func validateProvisionedIdentitySeparation(coordinator string) error {
	users := make(map[string]string, 3)
	for _, name := range []string{coordinator, workerUser, reviewerUser} {
		u, err := hostLookupUser(name)
		if err != nil || u.Uid == "" {
			return fmt.Errorf("resolve %s identity", name)
		}
		if previous := users[u.Uid]; previous != "" {
			return fmt.Errorf("%s and %s share UID %s", previous, name, u.Uid)
		}
		users[u.Uid] = name
	}
	attempt, err := hostLookupGroup(attemptGroup)
	if err != nil || attempt.Gid == "" {
		return fmt.Errorf("resolve %s identity", attemptGroup)
	}
	snapshot, err := hostLookupGroup(snapshotGroup)
	if err != nil || snapshot.Gid == "" {
		return fmt.Errorf("resolve %s identity", snapshotGroup)
	}
	if attempt.Gid == snapshot.Gid {
		return fmt.Errorf("%s and %s share GID %s", attemptGroup, snapshotGroup, attempt.Gid)
	}
	return nil
}

func rollbackSudoers(path string, previous []byte, existed bool) error {
	if !existed {
		return os.Remove(path)
	}
	return restoreSudoers(path, previous)
}

func restoreSudoers(path string, body []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".agent-symphony-rollback-")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer os.Remove(name)
	if _, err = tmp.Write(body); err == nil {
		err = tmp.Sync()
	}
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Chmod(name, 0o440)
	}
	if err == nil {
		err = os.Chown(name, 0, 0)
	}
	if err != nil {
		return err
	}
	return os.Rename(name, path)
}

func installableSudoAuthority(body []byte, binary string) bool {
	for _, line := range strings.Split(string(body), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "(") {
			return exactSudoAuthority(body, binary) || exactSudoAuthorityFor(body, binary, true, true) || exactSudoAuthorityFor(body, binary, false, false)
		}
	}
	return true
}

func exactSudoAuthority(body []byte, binary string) bool {
	return exactSudoAuthorityFor(body, binary, true, false)
}

func exactSudoAuthorityFor(body []byte, binary string, orchestrator, setenv bool) bool {
	want := map[string]bool{
		workerUser + ":" + attemptGroup + "\x00" + binary + " agent-host implementation": false,
		reviewerUser + ":" + snapshotGroup + "\x00" + binary + " agent-host review":      false,
	}
	if orchestrator {
		want[reviewerUser+":"+snapshotGroup+"\x00"+binary+" agent-host orchestrator"] = false
	}
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "(") {
			continue
		}
		runas, command, ok := strings.Cut(line, ") NOPASSWD:")
		if !ok {
			return false
		}
		hasSetenv := strings.HasPrefix(command, "SETENV:")
		if hasSetenv {
			command = strings.TrimPrefix(command, "SETENV:")
		}
		if hasSetenv != setenv {
			return false
		}
		parts := strings.Split(strings.TrimPrefix(runas, "("), ":")
		if len(parts) != 2 {
			return false
		}
		key := strings.TrimSpace(parts[0]) + ":" + strings.TrimSpace(parts[1]) + "\x00" + strings.TrimSpace(command)
		if _, ok := want[key]; !ok || want[key] {
			return false
		}
		want[key] = true
	}
	valid := want[workerUser+":"+attemptGroup+"\x00"+binary+" agent-host implementation"] && want[reviewerUser+":"+snapshotGroup+"\x00"+binary+" agent-host review"]
	return valid && (!orchestrator || want[reviewerUser+":"+snapshotGroup+"\x00"+binary+" agent-host orchestrator"])
}

func validateInstalledBinary(binary string) error {
	prefix := "/usr/local/libexec/agent-symphony/"
	rel := strings.TrimPrefix(binary, prefix)
	if rel == binary || rel == "agent-symphony" || filepath.Dir(rel) == "." || filepath.Base(binary) != "agent-symphony" || strings.Contains(filepath.Dir(rel), string(os.PathSeparator)) {
		return errors.New("current binary must use /usr/local/libexec/agent-symphony/<version>/agent-symphony")
	}
	for path := binary; ; path = filepath.Dir(path) {
		info, err := os.Lstat(path)
		if err != nil || info.Mode()&os.ModeSymlink != 0 || fileUID(info) != 0 || fileGID(info) != 0 || info.Mode().Perm()&0o022 != 0 {
			return fmt.Errorf("installed binary path component %s must be root:root and not group/world writable", path)
		}
		if path == binary && (!info.Mode().IsRegular() || info.Mode().Perm() != 0o755) {
			return errors.New("current binary must be a root-owned regular file mode 0755")
		}
		if path == string(os.PathSeparator) {
			break
		}
	}
	return nil
}

func ensureHostRoot(path, owner, group, mode string) error {
	if info, err := os.Stat(path); err == nil {
		u, userErr := hostLookupUser(owner)
		g, groupErr := hostLookupGroup(group)
		want, modeErr := strconv.ParseUint(mode, 8, 32)
		actual := uint64(info.Mode().Perm())
		if info.Mode()&os.ModeSetgid != 0 {
			actual |= 0o2000
		}
		if userErr != nil || groupErr != nil || modeErr != nil || !info.IsDir() || fileUID(info) != atoi(u.Uid) || fileGID(info) != atoi(g.Gid) || actual != want {
			return fmt.Errorf("%s has conflicting ownership or mode", path)
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	parent := filepath.Dir(path)
	if err := hostRun("mkdir", "-p", parent); err != nil {
		return err
	}
	tmp, err := os.MkdirTemp(parent, ".agent-symphony-root-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	if err := hostRun("chown", owner+":"+group, tmp); err != nil {
		return err
	}
	if err := hostRun("chmod", mode, tmp); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		if _, statErr := os.Stat(path); statErr == nil {
			return ensureHostRoot(path, owner, group, mode)
		}
		return err
	}
	return nil
}

func provisionIdentities(coordinator string) error {
	if hostGOOS == "linux" {
		for _, group := range []string{attemptGroup, snapshotGroup} {
			if _, err := hostLookupGroup(group); err != nil && hostRun("groupadd", "--system", group) != nil {
				return fmt.Errorf("create group %s", group)
			}
			if err := validateHostGroup(group, coordinator); err != nil {
				return err
			}
		}
		for _, account := range []struct{ user, group string }{{workerUser, attemptGroup}, {reviewerUser, snapshotGroup}} {
			if _, err := hostLookupUser(account.user); err != nil {
				if err := hostRun("useradd", "--system", "--gid", account.group, "--home-dir", "/var/lib/"+account.user, "--create-home", "--shell", "/usr/sbin/nologin", account.user); err != nil {
					return err
				}
			}
			if err := validateIdentity(account.user, account.group); err != nil {
				return err
			}
		}
		return hostRun("usermod", "--append", "--groups", attemptGroup+","+snapshotGroup, coordinator)
	}
	nextID, err := nextDarwinID()
	if err != nil {
		return err
	}
	for _, group := range []string{attemptGroup, snapshotGroup} {
		existing, lookupErr := hostLookupGroup(group)
		preparing := darwinRecordPreparing("/Groups/" + group)
		if lookupErr != nil || preparing {
			gid := strconv.Itoa(nextID)
			if preparing && existing != nil && existing.Gid != "" {
				gid = existing.Gid
			} else {
				nextID++
			}
			if err := ensureDarwinRecord("/Groups/"+group, [][2]string{{"PrimaryGroupID", gid}, {"Password", "*"}}); err != nil {
				return err
			}
		}
		if err := validateHostGroup(group, coordinator); err != nil {
			return err
		}
	}
	for _, account := range []struct{ user, group string }{{workerUser, attemptGroup}, {reviewerUser, snapshotGroup}} {
		existing, lookupErr := hostLookupUser(account.user)
		preparing := darwinRecordPreparing("/Users/" + account.user)
		if lookupErr != nil || preparing {
			group, groupErr := hostLookupGroup(account.group)
			if groupErr != nil {
				return groupErr
			}
			home, uid := "/var/db/"+account.user, strconv.Itoa(nextID)
			if preparing && existing != nil && existing.Uid != "" {
				uid = existing.Uid
			} else {
				nextID++
			}
			if err := ensureDarwinRecord("/Users/"+account.user, [][2]string{{"UniqueID", uid}, {"PrimaryGroupID", group.Gid}, {"NFSHomeDirectory", home}, {"UserShell", "/usr/bin/false"}, {"IsHidden", "1"}}); err != nil {
				return err
			}
		}
		home := "/var/db/" + account.user
		if err := hostRun("mkdir", "-p", home); err != nil {
			return err
		}
		if err := hostRun("chown", account.user+":"+account.group, home); err != nil {
			return err
		}
		if err := validateIdentity(account.user, account.group); err != nil {
			return err
		}
	}
	for _, group := range []string{attemptGroup, snapshotGroup} {
		if err := hostRun("dseditgroup", "-o", "edit", "-a", coordinator, "-t", "user", group); err != nil {
			return err
		}
	}
	return nil
}

func darwinRecordPreparing(path string) bool {
	out, err := hostOutput("dscl", ".", "-read", path, "AgentSymphonyPreparing")
	return err == nil && parseDSCLRecord(out)["AgentSymphonyPreparing"] == "1"
}

func ensureDarwinRecord(path string, properties [][2]string) error {
	rollback := func(cause error) error { return errors.Join(cause, hostRun("dscl", ".", "-delete", path)) }
	if err := hostRun("dscl", ".", "-create", path, "AgentSymphonyPreparing", "1"); err != nil {
		return rollback(err)
	}
	for _, property := range properties {
		if err := hostRun("dscl", ".", "-create", path, property[0], property[1]); err != nil {
			return rollback(err)
		}
	}
	if err := hostRun("dscl", ".", "-delete", path, "AgentSymphonyPreparing"); err != nil {
		return rollback(err)
	}
	return nil
}

func validateHostGroup(name, coordinator string) error {
	g, err := hostLookupGroup(name)
	if err != nil || g.Gid == "" || g.Gid == "0" {
		return fmt.Errorf("%s has unsafe group identity", name)
	}
	if hostGOOS == "linux" {
		record, outputErr := hostOutput("getent", "group", name)
		fields := strings.Split(strings.TrimSpace(string(record)), ":")
		if outputErr != nil || len(fields) != 4 || fields[0] != name || fields[2] != g.Gid || (fields[3] != "" && fields[3] != coordinator) {
			return fmt.Errorf("%s has conflicting GID or members", name)
		}
	} else {
		record, outputErr := hostOutput("dscl", ".", "-read", "/Groups/"+name, "PrimaryGroupID", "Password", "GroupMembership")
		properties := parseDSCLRecord(record)
		gid, gidErr := strconv.Atoi(g.Gid)
		members := properties["GroupMembership"]
		if outputErr != nil || gidErr != nil || gid < 400 || gid > 499 || properties["PrimaryGroupID"] != g.Gid || properties["Password"] != "*" || (members != "" && members != coordinator) {
			return fmt.Errorf("%s has conflicting macOS group properties", name)
		}
	}
	return nil
}

func nextDarwinID() (int, error) {
	used := map[int]bool{}
	for _, query := range [][]string{{".", "-list", "/Users", "UniqueID"}, {".", "-list", "/Groups", "PrimaryGroupID"}} {
		out, err := hostOutput("dscl", query...)
		if err != nil {
			return 0, err
		}
		for _, line := range strings.Split(string(out), "\n") {
			fields := strings.Fields(line)
			if len(fields) == 2 {
				id, parseErr := strconv.Atoi(fields[1])
				if parseErr == nil {
					used[id] = true
				}
			}
		}
	}
	for id := 400; id < 500; id++ {
		if !used[id] {
			return id, nil
		}
	}
	return 0, errors.New("no unused hidden macOS identity ID in 400-499")
}

func validateIdentity(name, group string) error {
	u, err := hostLookupUser(name)
	if err != nil {
		return err
	}
	g, err := hostLookupGroup(group)
	if err != nil {
		return err
	}
	if u.Gid != g.Gid {
		return fmt.Errorf("%s has conflicting primary group", name)
	}
	wantHome, wantShell := "/var/lib/"+name, "/usr/sbin/nologin"
	if hostGOOS == "darwin" {
		wantHome, wantShell = "/var/db/"+name, "/usr/bin/false"
	}
	if u.Uid == "0" || u.Uid == "" || u.HomeDir != wantHome {
		return fmt.Errorf("%s has unsafe identity", name)
	}
	if hostGOOS == "linux" {
		passwd, outputErr := hostOutput("getent", "passwd", name)
		fields := strings.Split(strings.TrimSpace(string(passwd)), ":")
		if outputErr != nil || len(fields) != 7 || fields[0] != name || fields[2] != u.Uid || fields[3] != g.Gid || fields[5] != wantHome || fields[6] != wantShell {
			return fmt.Errorf("%s has conflicting home, shell, UID, or GID", name)
		}
	} else {
		record, outputErr := hostOutput("dscl", ".", "-read", "/Users/"+name, "UniqueID", "PrimaryGroupID", "NFSHomeDirectory", "UserShell", "IsHidden")
		properties := parseDSCLRecord(record)
		uid, uidErr := strconv.Atoi(u.Uid)
		if outputErr != nil || uidErr != nil || uid < 400 || uid > 499 || properties["UniqueID"] != u.Uid || properties["PrimaryGroupID"] != g.Gid || properties["NFSHomeDirectory"] != wantHome || properties["UserShell"] != wantShell || properties["IsHidden"] != "1" {
			return fmt.Errorf("%s has conflicting macOS identity properties", name)
		}
	}
	groups, err := hostOutput("id", "-G", name)
	if err != nil || strings.Join(strings.Fields(string(groups)), " ") != g.Gid {
		return fmt.Errorf("%s has unsafe supplementary groups", name)
	}
	return nil
}

func parseDSCLRecord(body []byte) map[string]string {
	result := map[string]string{}
	for _, line := range strings.Split(string(body), "\n") {
		if name, value, ok := strings.Cut(strings.TrimSpace(line), ":"); ok {
			result[name] = strings.TrimSpace(value)
		}
	}
	return result
}

func writeSudoers(coordinator, binary string) (bool, error) {
	body := sudoersPolicy(coordinator, binary)
	dir, path := nativeRoot("/etc/sudoers.d"), nativeRoot("/etc/sudoers.d/agent-symphony")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return false, err
	}
	tmp, err := os.CreateTemp(dir, ".agent-symphony-")
	if err != nil {
		return false, err
	}
	name := tmp.Name()
	defer os.Remove(name)
	if _, err = io.WriteString(tmp, body); err == nil {
		err = tmp.Sync()
	}
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return false, err
	}
	if err := os.Chmod(name, 0o440); err != nil {
		return false, err
	}
	if err := os.Chown(name, 0, 0); err != nil {
		return false, err
	}
	if err := hostRun("visudo", "-cf", name); err != nil {
		return false, err
	}
	_, statErr := os.Lstat(path)
	return errors.Is(statErr, os.ErrNotExist), os.Rename(name, path)
}

func sudoersPolicy(coordinator, binary string) string {
	return fmt.Sprintf("# managed by agent-symphony; rerun install-host after upgrades\nDefaults!%s env_keep += \"%s\"\n%s ALL=(%s:%s) NOPASSWD: %s agent-host implementation\n%s ALL=(%s:%s) NOPASSWD: %s agent-host review\n%s ALL=(%s:%s) NOPASSWD: %s agent-host orchestrator\n", binary, strings.Join(internalgithub.GitHubCLIEnvironmentNames(), " "), coordinator, workerUser, attemptGroup, binary, coordinator, reviewerUser, snapshotGroup, binary, coordinator, reviewerUser, snapshotGroup, binary)
}

type reviewResultRequest struct {
	Repository         string `json:"repository"`
	Issue              int    `json:"issue"`
	Attempt            int    `json:"attempt"`
	Mode               string `json:"mode"`
	Target             string `json:"target"`
	Head               string `json:"head"`
	LegacyHeadArtifact bool   `json:"legacy_head_artifact,omitempty"`
}

const (
	maxReviewResultSize     = 64 << 10
	reviewResultInvalidCode = 65
)

func readReviewResult(input []byte, root string) (string, error) {
	var request reviewResultRequest
	decoder := json.NewDecoder(bytes.NewReader(input))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&request) != nil || decoder.Decode(&struct{}{}) != io.EOF || request.Issue <= 0 || request.Attempt <= 0 || !preflightObjectID.MatchString(request.Head) {
		return "", errors.New("invalid review result request")
	}
	parts := strings.Split(request.Repository, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" || len(request.Repository) > 256 || strings.ContainsAny(request.Repository, "\\\x00\r\n") {
		return "", errors.New("invalid review result request")
	}
	if !agentruntime.ValidReviewMetadata(request.Mode, request.Target) || !validReviewTarget(request.Mode, request.Target, request.Repository, request.Issue, request.Head) || request.LegacyHeadArtifact && request.Mode != agentruntime.ReviewModeImplementation {
		return "", errors.New("invalid review result request")
	}
	snapshot, _ := reviewIdentity(agentruntime.Attempt{Repository: request.Repository, Issue: request.Issue, Number: request.Attempt}, root)
	path := reviewResultPath(snapshot, request.Target)
	if !belowRoot(path, root) {
		return "", errors.New("review result path escapes snapshot root")
	}
	listed, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) && request.LegacyHeadArtifact {
		path = reviewResultPath(snapshot, request.Head)
		listed, err = os.Lstat(path)
	}
	if err != nil || !listed.Mode().IsRegular() || listed.Size() <= 0 || listed.Size() > maxReviewResultSize {
		return "", errors.New("review result artifact is missing, unsafe, or oversized")
	}
	fd, err := hostReviewResultOpen(path, syscall.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return "", errors.New("open review result artifact without following links")
	}
	file := os.NewFile(uintptr(fd), path)
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(listed, opened) || opened.Size() <= 0 || opened.Size() > maxReviewResultSize {
		return "", errors.New("review result artifact changed during validation")
	}
	body, err := io.ReadAll(io.LimitReader(file, maxReviewResultSize+1))
	if err != nil || len(body) == 0 || len(body) > maxReviewResultSize {
		return "", errors.New("review result artifact is missing or oversized")
	}
	return internalgithub.RedactEnvironment(string(body), os.Environ()), nil
}

func runHostOrchestrator(ctx context.Context, root, home string, local bool) error {
	dir, err := hostGetwd()
	if err != nil || !belowRoot(dir, root) || !strings.HasPrefix(filepath.Base(dir), "orchestrator-") {
		return errors.New("orchestrator workspace is outside the reviewer boundary")
	}
	path := filepath.Join(dir, orchestratorLaunchFile)
	listed, err := os.Lstat(path)
	if err != nil || !listed.Mode().IsRegular() || listed.Mode()&os.ModeSymlink != 0 || listed.Mode().Perm() != 0o440 || fileGID(listed) != hostEGID() || listed.Size() <= 0 || listed.Size() > 128<<10 {
		return errors.New("orchestrator launch contract is unsafe")
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(listed, opened) || opened.Mode().Perm() != 0o440 || fileGID(opened) != hostEGID() {
		return errors.New("orchestrator launch contract changed while opening")
	}
	body, err := io.ReadAll(io.LimitReader(file, 128<<10+1))
	if err != nil || len(body) > 128<<10 {
		return errors.New("orchestrator launch contract is oversized")
	}
	var launch struct {
		Version int      `json:"version"`
		Command []string `json:"command"`
		Context string   `json:"context"`
		OneShot bool     `json:"one_shot,omitempty"`
		Timeout int      `json:"timeout_seconds,omitempty"`
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&launch) != nil || decoder.Decode(&struct{}{}) != io.EOF || launch.Version != 1 || len(launch.Command) == 0 || len(launch.Command) > 128 || strings.TrimSpace(launch.Command[0]) == "" || len(launch.Context) == 0 || len(launch.Context) > 64<<10 {
		return errors.New("invalid orchestrator launch contract")
	}
	for _, arg := range launch.Command {
		if strings.ContainsAny(arg, "\x00\r\n") || credentialShapedArgument(arg) {
			return errors.New("unsafe orchestrator command argument")
		}
	}
	env, err := internalgithub.AgentEnvironmentWith(os.Environ())
	if err != nil {
		return err
	}
	env = append(env, "HOME="+home)
	if local {
		env = append(env, "AGENT_SYMPHONY_ORCHESTRATOR_ROOT="+root)
	}
	command := agentruntime.Command{Name: launch.Command[0], Args: slices.Clone(launch.Command[1:]), Dir: dir, Env: env}
	if launch.OneShot {
		if launch.Timeout < 1 || launch.Timeout > 300 {
			return errors.New("invalid one-shot orchestrator timeout")
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(launch.Timeout)*time.Second)
		defer cancel()
		command.Stdin = strings.NewReader(launch.Context)
	} else if launch.Timeout == 0 {
		command.Args = append(command.Args, launch.Context)
	} else {
		return errors.New("interactive orchestrator cannot set a timeout")
	}
	return hostOrchestratorRun(ctx, command)
}

func writeHostOrchestratorProposal(root string, input io.Reader, output io.Writer) error {
	dir, err := hostGetwd()
	if err != nil || !belowRoot(dir, root) || !strings.HasPrefix(filepath.Base(dir), "orchestrator-") {
		return errors.New("orchestrator workspace is outside the reviewer boundary")
	}
	proposal, canonical, err := parseHostOrchestratorProposal(input)
	if err != nil {
		return err
	}
	path := filepath.Join(dir, orchestratoragent.MessageProposalFile)
	listed, err := os.Lstat(path)
	if err != nil || !listed.Mode().IsRegular() || listed.Mode()&os.ModeSymlink != 0 || listed.Mode().Perm() != 0o620 || fileGID(listed) != hostEGID() || listed.Size() > 64<<10 {
		return errors.New("orchestrator proposal file is unsafe")
	}
	file, err := os.OpenFile(path, os.O_WRONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX); err != nil {
		file.Close()
		return err
	}
	opened, statErr := file.Stat()
	if statErr != nil || !os.SameFile(listed, opened) {
		file.Close()
		return errors.New("orchestrator proposal file changed while opening")
	}
	if err := file.Truncate(0); err != nil {
		file.Close()
		return err
	}
	if _, err := file.Write(append(canonical, '\n')); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return json.NewEncoder(output).Encode(struct {
		Version int    `json:"version"`
		Binding string `json:"binding"`
		State   string `json:"state"`
	}{1, proposal.Binding, "submitted"})
}

func parseHostOrchestratorProposal(input io.Reader) (orchestratoragent.MessageProposal, []byte, error) {
	body, err := io.ReadAll(io.LimitReader(input, 64<<10+1))
	if err != nil || len(body) == 0 || len(body) > 64<<10 {
		return orchestratoragent.MessageProposal{}, nil, errors.New("invalid bounded orchestrator proposal")
	}
	var proposal struct {
		Version    int    `json:"version"`
		Repository string `json:"repository"`
		Issue      int    `json:"issue"`
		Attempt    int    `json:"attempt"`
		Action     string `json:"action,omitempty"`
		Message    string `json:"message,omitempty"`
		RequestID  string `json:"request_id,omitempty"`
		HandoffID  string `json:"handoff_id,omitempty"`
		Detail     string `json:"detail,omitempty"`
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&proposal) != nil || decoder.Decode(&struct{}{}) != io.EOF || proposal.Version != 1 {
		return orchestratoragent.MessageProposal{}, nil, errors.New("invalid orchestrator proposal schema")
	}
	parsed := orchestratoragent.MessageProposal{Version: proposal.Version, Repository: proposal.Repository, Issue: proposal.Issue, Attempt: proposal.Attempt, Action: proposal.Action, Message: proposal.Message, RequestID: proposal.RequestID, HandoffID: proposal.HandoffID, Detail: proposal.Detail}
	if err := orchestratoragent.ValidateMessageProposal(parsed); err != nil {
		return orchestratoragent.MessageProposal{}, nil, err
	}
	canonical, _ := json.Marshal(proposal)
	if parsed.Action == "" {
		parsed.Action = orchestratoragent.ProposalActionMessage
	}
	parsed.Binding = fmt.Sprintf("%x", sha256.Sum256(canonical))
	return parsed, canonical, nil
}

func reportHostOrchestratorProposalStatus(root string, input io.Reader, output io.Writer) error {
	dir, err := hostGetwd()
	if err != nil || !belowRoot(dir, root) || !strings.HasPrefix(filepath.Base(dir), "orchestrator-") {
		return errors.New("orchestrator workspace is outside the reviewer boundary")
	}
	proposal, _, err := parseHostOrchestratorProposal(input)
	if err != nil {
		return err
	}
	status, present, err := readHostOrchestratorProposalStatus(filepath.Join(dir, orchestratoragent.MessageProposalStatusFile))
	if err != nil {
		return err
	}
	result := struct {
		Version    int        `json:"version"`
		Binding    string     `json:"binding"`
		State      string     `json:"state"`
		ObservedAt *time.Time `json:"observed_at,omitempty"`
		Detail     string     `json:"detail"`
	}{Version: 1, Binding: proposal.Binding, State: "unknown", Detail: "no matching coordinator observation is available"}
	if present {
		result.ObservedAt = &status.UpdatedAt
		switch {
		case status.ResolvedBinding == proposal.Binding && status.Resolution != "":
			result.State, result.Detail = status.Resolution, status.Detail
		case status.PendingBinding == proposal.Binding:
			result.State, result.Detail = "pending", "the coordinator captured this exact proposal and has not resolved it"
		case status.ConsumedBinding == proposal.Binding:
			result.State, result.Detail = "consumed", "the coordinator consumed this exact proposal; confirmation, cancellation, queueing, and delivery are not distinguished here"
		case status.PendingBinding != "":
			result.State, result.Detail = "replaced", "a different proposal is currently pending"
		}
	}
	return json.NewEncoder(output).Encode(result)
}

func readHostOrchestratorProposalStatus(path string) (orchestratoragent.MessageProposalStatus, bool, error) {
	listed, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return orchestratoragent.MessageProposalStatus{}, false, nil
	}
	if err != nil || !listed.Mode().IsRegular() || listed.Mode()&os.ModeSymlink != 0 || listed.Mode().Perm() != 0o440 || fileGID(listed) != hostEGID() || listed.Size() <= 0 || listed.Size() > 4<<10 {
		return orchestratoragent.MessageProposalStatus{}, false, errors.New("orchestrator proposal status is unsafe")
	}
	file, err := os.Open(path)
	if err != nil {
		return orchestratoragent.MessageProposalStatus{}, false, err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(listed, opened) {
		return orchestratoragent.MessageProposalStatus{}, false, errors.New("orchestrator proposal status changed while opening")
	}
	body, err := io.ReadAll(io.LimitReader(file, 4<<10+1))
	if err != nil || int64(len(body)) != opened.Size() || len(body) > 4<<10 {
		return orchestratoragent.MessageProposalStatus{}, false, errors.New("orchestrator proposal status changed while reading")
	}
	var status orchestratoragent.MessageProposalStatus
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&status) != nil || decoder.Decode(&struct{}{}) != io.EOF || status.Version != 1 || status.UpdatedAt.IsZero() || !validProposalBinding(status.PendingBinding) || !validProposalBinding(status.ConsumedBinding) || !validProposalBinding(status.ResolvedBinding) || !validProposalResolution(status.Resolution) || (status.Resolution == "") != (status.ResolvedBinding == "") {
		return orchestratoragent.MessageProposalStatus{}, false, errors.New("orchestrator proposal status is invalid")
	}
	return status, true, nil
}

func validProposalBinding(value string) bool {
	if value == "" {
		return true
	}
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func validProposalResolution(value string) bool {
	return value == "" || slices.Contains([]string{"running", "succeeded", "failed", "refused", "accepted"}, value)
}

func credentialShapedArgument(value string) bool {
	lower := strings.ToLower(value)
	for _, part := range []string{"authorization", "token", "secret", "password", "passwd", "private_key", "private-key", "credential", "api_key", "api-key", "github_pat"} {
		if strings.Contains(lower, part) {
			return true
		}
	}
	return false
}

func agentHost(ctx context.Context, mode string, input io.Reader, output io.Writer) error {
	localRoot := strings.TrimSpace(os.Getenv("AGENT_SYMPHONY_LOCAL_ROOT"))
	if localRoot != "" && hostIsolationInstalled() {
		return errors.New("local boundary root is disabled when host isolation is installed")
	}
	wantUser, wantGroup, root := workerUser, attemptGroup, "/var/lib/agent-symphony/attempts"
	if hostGOOS == "darwin" {
		root = "/var/db/agent-symphony/attempts"
	}
	orchestratorMode := mode == "orchestrator"
	orchestratorProposalMode := mode == "orchestrator-proposal"
	orchestratorProposalStatusMode := mode == "orchestrator-proposal-status"
	if (orchestratorProposalMode || orchestratorProposalStatusMode) && localRoot == "" {
		localRoot = strings.TrimSpace(os.Getenv("AGENT_SYMPHONY_ORCHESTRATOR_ROOT"))
	}
	if mode == "review" || orchestratorMode || orchestratorProposalMode || orchestratorProposalStatusMode {
		wantUser, wantGroup = reviewerUser, snapshotGroup
		root = strings.Replace(root, "attempts", "snapshots", 1)
	} else if mode != "implementation" {
		return errors.New("agent-host mode must be implementation, review, orchestrator, orchestrator-proposal, or orchestrator-proposal-status")
	}
	// AGENT_SYMPHONY_LOCAL_ROOT is only ever set by the coordinator's own
	// implementationBoundary/reviewBoundary when install-host was never run;
	// there is no separate OS identity to verify in that mode, only the same
	// user the coordinator itself runs as.
	var homeDir string
	var err error
	if localRoot != "" {
		if !filepath.IsAbs(localRoot) {
			return errors.New("local boundary root must be absolute")
		}
		root = localRoot
		var current *user.User
		if current, err = hostCurrentUser(); err != nil {
			return err
		}
		homeDir = current.HomeDir
	} else {
		root = nativeRoot(root)
		var u *user.User
		var g *user.Group
		if u, err = hostLookupUser(wantUser); err != nil {
			return err
		}
		if g, err = hostLookupGroup(wantGroup); err != nil {
			return err
		}
		uid, _ := strconv.Atoi(u.Uid)
		gid, _ := strconv.Atoi(g.Gid)
		if hostEUID() != uid || hostEGID() != gid {
			return fmt.Errorf("agent-host must run as %s:%s", wantUser, wantGroup)
		}
		homeDir = u.HomeDir
	}
	if orchestratorMode {
		return runHostOrchestrator(ctx, root, homeDir, localRoot != "")
	}
	if orchestratorProposalMode {
		return writeHostOrchestratorProposal(root, input, output)
	}
	if orchestratorProposalStatusMode {
		return reportHostOrchestratorProposalStatus(root, input, output)
	}
	var request struct {
		Operation string          `json:"operation"`
		Command   boundaryCommand `json:"command"`
	}
	body, readErr := io.ReadAll(io.LimitReader(input, 1<<20+1))
	if readErr != nil || len(body) > 1<<20 {
		return errors.New("invalid bounded JSON request")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&request) != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return errors.New("invalid bounded JSON request")
	}
	var result agentruntime.Result
	redactionEnv := append(os.Environ(), request.Command.Env...)
	switch request.Operation {
	case "verify":
		if localRoot != "" {
			if err := verifyLocalAccess(root); err != nil {
				return err
			}
		} else if hostRoot == "" {
			var canary struct {
				Deny     []string `json:"deny"`
				Snapshot string   `json:"snapshot"`
			}
			if len(request.Command.Input) != 0 && json.Unmarshal(request.Command.Input, &canary) != nil {
				return errors.New("invalid host canary request")
			}
			if err := verifyHostAccess(root, mode, canary.Deny, canary.Snapshot); err != nil {
				return err
			}
		}
	case "run":
		if err := validateBoundaryCommand(request.Command, root); err != nil {
			return err
		}
		names := make([]string, 0, len(request.Command.Env))
		for _, value := range request.Command.Env {
			names = append(names, strings.SplitN(value, "=", 2)[0])
		}
		env, filterErr := internalgithub.AgentEnvironmentWith(request.Command.Env, names...)
		if filterErr != nil {
			return filterErr
		}
		env = append(env, "HOME="+homeDir)
		dir := request.Command.Dir
		args := request.Command.Args
		if request.Command.Name == "tmux" {
			dir = "/tmp"
			if tmuxRoot := os.Getenv("TMUX_TMPDIR"); localRoot != "" && tmuxRoot != "" {
				env = append(env, "TMUX_TMPDIR="+tmuxRoot)
			}
			if tmuxNewSessionOffset(args) >= 0 {
				args = append(slices.Clone(args), "-e", "HOME="+homeDir)
			}
		}
		result, err = hostExecRunner(ctx, agentruntime.Command{Name: request.Command.Name, Args: args, Dir: dir, Env: env, Stdin: bytes.NewReader(request.Command.Input)})
	case "export":
		if mode != "implementation" {
			return errors.New("review boundary cannot export implementation attempts")
		}
		result.Output, err = exportAttempt(ctx, request.Command.Input, root)
	case "cleanup":
		if mode != "implementation" {
			return errors.New("review boundary cannot clean implementation attempts")
		}
		err = cleanupAttempt(ctx, request.Command.Input, root)
	case "abandon":
		if mode != "implementation" {
			return errors.New("review boundary cannot abandon implementation attempts")
		}
		err = abandonAttempt(ctx, request.Command.Input, root)
	case "accept-handoff":
		if mode != "implementation" {
			return errors.New("review boundary cannot accept implementation handoffs")
		}
		result.Output, err = acceptHandoff(ctx, request.Command.Input, root)
	case "accept-operator-handoff":
		if mode != "implementation" {
			return errors.New("review boundary cannot accept operator handoffs")
		}
		result.Output, err = acceptOperatorHandoff(ctx, request.Command.Input, root)
	case "verify-handoff":
		if mode != "implementation" {
			return errors.New("review boundary cannot verify implementation handoffs")
		}
		result.Output, err = verifyHandoff(ctx, request.Command.Input, root)
	case "review-result":
		if mode != "review" {
			return errors.New("implementation boundary cannot read review results")
		}
		result.Output, err = readReviewResult(request.Command.Input, root)
		if err != nil {
			result = agentruntime.Result{Output: "review result artifact is invalid", Code: reviewResultInvalidCode, Exited: true}
		}
	default:
		return errors.New("unsupported boundary operation")
	}
	result.Output = internalgithub.RedactEnvironment(result.Output, redactionEnv)
	if err != nil && !result.Exited {
		return errors.New(internalgithub.RedactEnvironment(err.Error(), redactionEnv))
	}
	return json.NewEncoder(output).Encode(result)
}

func cleanupAttempt(ctx context.Context, input []byte, root string) error {
	return removeAttemptResources(ctx, input, root, true)
}

func abandonAttempt(ctx context.Context, input []byte, root string) error {
	return removeAttemptResources(ctx, input, root, false)
}

func removeAttemptResources(ctx context.Context, input []byte, root string, completed bool) error {
	var manifest agentruntime.Manifest
	decoder := json.NewDecoder(bytes.NewReader(input))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&manifest) != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return errors.New("invalid attempt manifest")
	}
	attempt := agentruntime.Attempt{Repository: manifest.Repository, Issue: manifest.Issue, Number: manifest.Attempt, BaseSHA: manifest.BaseSHA}
	want, err := agentruntime.AttemptIdentity(root, attempt)
	validState := manifest.State == "preparing" || manifest.State == "running" || manifest.State == "completed" || manifest.State == "failed" || manifest.State == "cancelled"
	if err != nil || manifest.Version != want.Version || !validState || manifest.Branch != want.Branch || manifest.Worktree != want.Worktree || manifest.Session != want.Session ||
		(completed && (manifest.State != "completed" || !preflightObjectID.MatchString(manifest.ReviewHead))) {
		return errors.New("invalid attempt manifest")
	}

	worktreeInfo, worktreeErr := os.Lstat(want.Worktree)
	if worktreeErr != nil && !errors.Is(worktreeErr, os.ErrNotExist) {
		return worktreeErr
	}
	if worktreeErr == nil {
		if !worktreeInfo.IsDir() || worktreeInfo.Mode()&os.ModeSymlink != 0 {
			return errors.New("cleanup worktree is not a non-symlink directory")
		}
		run := func(args ...string) (string, error) {
			command := exec.CommandContext(ctx, "git", append([]string{"--no-optional-locks", "-c", "core.hooksPath=/dev/null", "-C", want.Worktree}, args...)...)
			command.Env = append(minimalBoundaryEnvironment(), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_TERMINAL_PROMPT=0")
			out, err := command.CombinedOutput()
			return strings.TrimSpace(string(out)), err
		}
		top, topErr := run("rev-parse", "--show-toplevel")
		gitDir, gitDirErr := run("rev-parse", "--absolute-git-dir")
		if topErr != nil || !samePath(top, want.Worktree) || gitDirErr != nil || !validAttemptGitDir(want.Worktree, gitDir, root) {
			return errors.New("attempt worktree identity changed")
		}
		if completed {
			branch, branchErr := run("branch", "--show-current")
			head, headErr := run("rev-parse", "HEAD")
			if branchErr != nil || branch != want.Branch || headErr != nil || head != manifest.ReviewHead {
				return errors.New("cleanup worktree identity changed")
			}
		}
	}

	resultPath := agentruntime.ResultPath(want.Worktree)
	resultInfo, resultErr := os.Lstat(resultPath)
	if resultErr != nil && !errors.Is(resultErr, os.ErrNotExist) {
		return resultErr
	}
	if resultErr == nil && (!resultInfo.Mode().IsRegular() || resultInfo.Mode()&os.ModeSymlink != 0) {
		return errors.New("cleanup result is not a regular non-symlink file")
	}
	if err := stopAttemptSession(ctx, want.Session); err != nil {
		return err
	}
	if worktreeErr == nil {
		if err := os.RemoveAll(want.Worktree); err != nil {
			return err
		}
	}
	if resultErr == nil {
		if err := os.Remove(resultPath); err != nil {
			return err
		}
	}
	return nil
}

func stopAttemptSession(ctx context.Context, session string) error {
	probe := func() (bool, error) {
		result, err := runHostTmux(ctx, []string{"has-session", "-t", "=" + session}, nil)
		if err == nil {
			return true, nil
		}
		if result.Exited && result.Code == 1 {
			return false, nil
		}
		return false, err
	}
	live, err := probe()
	if err != nil || !live {
		return err
	}
	if _, err := runHostTmux(ctx, []string{"kill-session", "-t", "=" + session}, nil); err != nil {
		if live, probeErr := probe(); probeErr != nil || live {
			return errors.Join(err, probeErr)
		}
	}
	live, err = probe()
	if err != nil {
		return err
	}
	if live {
		return errors.New("tmux session remained after cleanup")
	}
	return nil
}

func verifyHostAccess(root, mode string, deny []string, snapshot string) error {
	f, err := os.Open(root)
	if err != nil {
		return fmt.Errorf("read provisioned %s root: %w", mode, err)
	}
	f.Close()
	other := strings.Replace(root, "attempts", "snapshots", 1)
	if mode == "review" {
		other = strings.Replace(root, "snapshots", "attempts", 1)
	}
	if f, err := os.Open(other); err == nil {
		f.Close()
		return errors.New("agent identity can access the other isolation root")
	}
	for _, path := range deny {
		if f, err := os.Open(path); err == nil {
			f.Close()
			return errors.New("agent identity can access coordinator state or socket canary")
		}
	}
	if mode == "implementation" {
		rootInfo, rootErr := os.Stat(root)
		for i := 0; i < 2; i++ {
			path := filepath.Join(root, fmt.Sprintf(".doctor-attempt-%d-%d-%d", os.Getpid(), time.Now().UnixNano(), i))
			oldUmask := syscall.Umask(0o007)
			err := os.Mkdir(path, 0o770)
			syscall.Umask(oldUmask)
			if err != nil {
				return fmt.Errorf("create setgid attempt canary: %w", err)
			}
			defer os.RemoveAll(path)
			info, err := os.Stat(path)
			if err != nil || rootErr != nil || info.Mode()&os.ModeSetgid == 0 || info.Mode().Perm() != 0o770 || fileGID(info) != fileGID(rootInfo) {
				return errors.New("attempt setgid inheritance or umask canary failed")
			}
		}
		if snapshot != "" {
			if f, err := os.Open(snapshot); err == nil {
				f.Close()
				return errors.New("worker can read reviewer snapshot")
			}
		}
		entries, err := os.ReadDir(root)
		if err != nil || len(entries) > 1000 {
			return errors.New("cannot inspect bounded attempt repositories")
		}
		for _, entry := range entries {
			worktree := filepath.Join(root, entry.Name())
			if !entry.IsDir() {
				continue
			}
			if _, err := os.Stat(filepath.Join(worktree, ".git")); err != nil {
				continue
			}
			remote, remoteErr := exec.Command("git", "-C", worktree, "remote").Output()
			helper, helperErr := exec.Command("git", "-C", worktree, "config", "--get", "credential.helper").Output()
			if remoteErr != nil || (helperErr != nil && !isExitCode(helperErr, 1)) || strings.TrimSpace(string(remote)) != "" || strings.TrimSpace(string(helper)) != "" {
				return errors.New("attempt repository has a remote or credential helper")
			}
		}
	} else if snapshot != "" {
		if b, err := os.ReadFile(snapshot); err != nil || string(b) != "agent-symphony-review-canary\n" {
			return errors.New("reviewer cannot read completed snapshot")
		}
		if f, err := os.OpenFile(snapshot, os.O_WRONLY, 0); err == nil {
			f.Close()
			return errors.New("reviewer can mutate completed snapshot")
		}
	}
	return nil
}

func isExitCode(err error, code int) bool {
	var exit *exec.ExitError
	return errors.As(err, &exit) && exit.ExitCode() == code
}

func validateBoundaryCommand(c boundaryCommand, root string) error {
	if (c.Name != "git" && c.Name != "tmux") || len(c.Args) > 128 || len(c.Env) > 64 || len(c.Input) > 1<<20 {
		return errors.New("boundary command is not allowed")
	}
	if c.Dir != "" {
		if !belowRoot(c.Dir, root) {
			return errors.New("boundary command directory escapes provisioned root")
		}
	}
	if (c.Name == "git" && !validGitBoundaryArgs(c.Args, c.Dir, root)) || (c.Name == "tmux" && !validTmuxBoundaryArgs(c.Args, c.Env, c.Dir, root)) {
		return errors.New("boundary command arguments are not allowed")
	}
	for _, value := range c.Env {
		name, _, ok := strings.Cut(value, "=")
		if !ok || name == "" || strings.ContainsAny(name, " \t\r\n") || reservedHostEnvironment(name) {
			return errors.New("invalid boundary environment")
		}
	}
	return nil
}

func boundedCommandPath(path, dir, root string) bool {
	if !filepath.IsAbs(path) {
		if dir == "" {
			return false
		}
		path = filepath.Join(dir, path)
	}
	return belowRoot(path, root)
}

func validGitBoundaryArgs(args []string, dir, root string) bool {
	if len(args) == 5 && slices.Equal(args[:3], []string{"clone", "--no-local", "--no-checkout"}) {
		return boundedCommandPath(args[3], dir, root) && boundedCommandPath(args[4], dir, root)
	}
	if len(args) < 3 || args[0] != "-C" || !boundedCommandPath(args[1], dir, root) {
		return false
	}
	rest := args[2:]
	return slices.Equal(rest, []string{"branch", "--show-current"}) ||
		slices.Equal(rest, []string{"rev-parse", "HEAD"}) ||
		(len(rest) == 4 && rest[0] == "fetch" && rest[1] == "--no-tags" && filepath.IsAbs(rest[2]) && strings.HasSuffix(rest[2], ".source.bundle") && boundedCommandPath(rest[2], dir, root) && rest[3] == "+refs/heads/*:refs/remotes/agent-symphony/*") ||
		(len(rest) == 4 && rest[0] == "merge-base" && rest[1] == "--is-ancestor" && preflightObjectID.MatchString(rest[2]) && preflightObjectID.MatchString(rest[3])) ||
		(len(rest) == 3 && rest[0] == "checkout" && rest[1] == "--detach") ||
		(len(rest) == 3 && rest[0] == "switch" && rest[1] == "-c") ||
		slices.Equal(rest, []string{"remote", "remove", "origin"}) ||
		slices.Equal(rest, []string{"config", "--local", "credential.helper", ""})
}

func validTmuxBoundaryArgs(args, environment []string, dir, root string) bool {
	if len(args) == 0 || strings.HasPrefix(args[0], "-") {
		return false
	}
	if offset := tmuxNewSessionOffset(args); offset >= 0 {
		if offset == 0 || !slices.Equal(strings.Fields(args[3]), environmentNames(environment)) {
			return false
		}
		args = args[offset:]
	}
	switch args[0] {
	case "new-session":
		return len(args) == 6 && args[1] == "-d" && args[2] == "-s" && args[4] == "-c" && boundedCommandPath(args[5], dir, root)
	case "has-session", "kill-session":
		return len(args) == 3 && args[1] == "-t" && validTmuxTarget(args[2], false)
	case "display-message":
		return len(args) == 5 && args[1] == "-p" && args[2] == "-t" && validTmuxTarget(args[3], true) && slices.Contains([]string{"#{pane_dead}", "#{pane_dead} #{pane_dead_status}", "#{pane_start_command}"}, args[4])
	case "capture-pane":
		return len(args) == 6 && slices.Equal(args[1:5], []string{"-p", "-S", "-", "-t"}) && validTmuxTarget(args[5], true)
	case "set-option":
		return len(args) == 6 && slices.Equal(args[1:3], []string{"-w", "-t"}) && validTmuxTarget(args[3], true) && ((args[4] == "remain-on-exit" && args[5] == "on") || (args[4] == "history-limit" && slices.Contains([]string{"5000", "65536"}, args[5])))
	case "respawn-pane":
		return len(args) > 5 && slices.Equal(args[1:3], []string{"-k", "-t"}) && validTmuxTarget(args[3], true) && args[4] == "--" && args[5] != ""
	case "split-window":
		return len(args) > 7 && slices.Equal(args[1:3], []string{"-d", "-t"}) && validTmuxTarget(args[3], true) && args[4] == "-c" && boundedCommandPath(args[5], dir, root) && args[6] == "--" && args[7] != ""
	case "kill-pane":
		return len(args) == 3 && args[1] == "-t" && validTmuxTarget(args[2], true)
	case "load-buffer":
		return len(args) == 4 && args[1] == "-b" && args[2] != "" && args[3] == "-"
	case "paste-buffer":
		return len(args) == 6 && slices.Equal(args[1:3], []string{"-d", "-b"}) && args[3] != "" && args[4] == "-t" && validTmuxTarget(args[5], true)
	case "send-keys":
		return len(args) == 4 && args[1] == "-t" && validTmuxTarget(args[2], true) && (args[3] == "C-c" || args[3] == "C-d" || args[3] == "Enter")
	default:
		return false
	}
}

func tmuxNewSessionOffset(args []string) int {
	if len(args) > 5 && slices.Equal(args[:3], []string{"set-option", "-g", "update-environment"}) && args[4] == ";" && args[5] == "new-session" {
		return 5
	}
	if len(args) > 0 && args[0] == "new-session" {
		return 0
	}
	return -1
}

func environmentNames(environment []string) []string {
	var names []string
	for _, entry := range environment {
		name, _, ok := strings.Cut(entry, "=")
		if ok && name != "" && !slices.Contains(names, name) {
			names = append(names, name)
		}
	}
	return names
}

func validTmuxTarget(target string, pane bool) bool {
	if !strings.HasPrefix(target, "=") || strings.ContainsAny(target, "/\\\x00\r\n") {
		return false
	}
	return !pane || strings.HasSuffix(target, ":0.0")
}

func reservedHostEnvironment(name string) bool {
	if internalgithub.GitHubCLIEnvironmentVariable(name) {
		return false
	}
	upper := strings.ToUpper(name)
	if upper == "HOME" || upper == "TMUX_TMPDIR" {
		return true
	}
	for _, part := range []string{"TOKEN", "SECRET", "PASSWORD", "PASSWD", "PRIVATE_KEY", "PRIVATE-KEY", "CREDENTIAL", "AUTHORIZATION", "GITHUB_PAT"} {
		if strings.Contains(upper, part) {
			return true
		}
	}
	for _, prefix := range []string{"GITHUB_", "GH_", "SSH_", "AWS_", "AZURE_", "GOOGLE_", "GCP_", "CLOUD_", "OCI_", "CLOUDFLARE_", "DIGITALOCEAN_", "GIT_ASKPASS", "APP_"} {
		if strings.HasPrefix(upper, prefix) {
			return true
		}
	}
	return strings.HasSuffix(upper, "_PROXY")
}

func exportAttempt(ctx context.Context, input []byte, root string) (string, error) {
	var manifest agentruntime.Manifest
	d := json.NewDecoder(bytes.NewReader(input))
	d.DisallowUnknownFields()
	if d.Decode(&manifest) != nil || d.Decode(&struct{}{}) != io.EOF {
		return "", errors.New("invalid export manifest")
	}
	want, identityErr := agentruntime.AttemptIdentity(root, agentruntime.Attempt{Repository: manifest.Repository, Issue: manifest.Issue, Number: manifest.Attempt, BaseSHA: manifest.BaseSHA})
	if identityErr != nil || manifest.Version != want.Version || manifest.State != "completed" || manifest.Branch != want.Branch || manifest.Worktree != want.Worktree || manifest.Session != want.Session {
		return "", errors.New("invalid export manifest")
	}
	run := func(args ...string) (string, error) {
		cmd := exec.CommandContext(ctx, "git", append([]string{"--no-optional-locks", "-c", "core.hooksPath=/dev/null", "-C", manifest.Worktree}, args...)...)
		cmd.Env = append(minimalBoundaryEnvironment(), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_TERMINAL_PROMPT=0")
		out, err := cmd.CombinedOutput()
		return strings.TrimSpace(string(out)), err
	}
	top, err := run("rev-parse", "--show-toplevel")
	if err != nil || !samePath(top, manifest.Worktree) {
		return "", errors.New("export worktree identity changed")
	}
	gitDir, err := run("rev-parse", "--absolute-git-dir")
	if err != nil || !validAttemptGitDir(manifest.Worktree, gitDir, root) {
		return "", errors.New("export git directory escapes provisioned root")
	}
	branch, err := run("branch", "--show-current")
	if err != nil || branch != manifest.Branch {
		return "", errors.New("export branch changed")
	}
	if remote, err := run("remote"); err != nil || remote != "" {
		return "", errors.New("export worktree has a remote")
	}
	if helper, err := run("config", "--get-all", "credential.helper"); err != nil && !isExitCode(err, 1) || helper != "" {
		return "", errors.New("export worktree has a credential helper")
	}
	if _, err := run("merge-base", "--is-ancestor", manifest.BaseSHA, "HEAD"); err != nil {
		return "", errors.New("export head does not descend from base")
	}
	resultPath := agentruntime.ResultPath(want.Worktree)
	if !belowRoot(resultPath, root) {
		return "", errors.New("worker result path escapes provisioned root")
	}
	result, err := readWorkerResult(resultPath)
	if err != nil {
		return "", err
	}
	status, err := run("status", "--porcelain", "--", ".", ":(exclude).agent-symphony")
	if err != nil {
		return "", errors.New("inspect export worktree")
	}
	if status != "" {
		if _, err := run("add", "--all", "--", ".", ":(exclude).agent-symphony"); err != nil {
			return "", fmt.Errorf("stage worker changes: %w", err)
		}
		if _, err := run("diff", "--cached", "--quiet"); err == nil || !isExitCode(err, 1) {
			return "", errors.New("worker changes could not be staged")
		}
		message := fmt.Sprintf("agent-symphony: issue #%d attempt %d", manifest.Issue, manifest.Attempt)
		if _, err := run("-c", "user.name=Agent Symphony", "-c", "user.email=agent-symphony@localhost", "-c", "commit.gpgSign=false", "commit", "--no-verify", "-m", message); err != nil {
			return "", fmt.Errorf("commit worker changes: %w", err)
		}
	}
	head, err := run("rev-parse", "HEAD")
	if err != nil {
		return "", err
	}
	if branch, err := run("branch", "--show-current"); err != nil || branch != manifest.Branch {
		return "", errors.New("export branch changed")
	}
	if status, err := run("status", "--porcelain", "--", ".", ":(exclude).agent-symphony"); err != nil || status != "" {
		return "", errors.New("export worktree is not clean")
	}
	tmp, err := os.CreateTemp("", "agent-symphony-export-*.bundle")
	if err != nil {
		return "", err
	}
	name := tmp.Name()
	tmp.Close()
	defer os.Remove(name)
	if out, err := run("bundle", "create", name, "HEAD"); err != nil {
		return "", fmt.Errorf("create export bundle: %w: %s", err, out)
	}
	bundle, err := os.ReadFile(name)
	if err != nil || len(bundle) > 16<<20 {
		return "", errors.New("export bundle is invalid or oversized")
	}
	exported := workerExport{Type: "agent-symphony-export-v1", Repository: manifest.Repository, Branch: manifest.Branch, BaseSHA: manifest.BaseSHA, HeadSHA: head, BundleSHA256: fmt.Sprintf("%x", sha256.Sum256(bundle)), Clean: true, Result: result, Bundle: base64.StdEncoding.EncodeToString(bundle)}
	b, _ := json.Marshal(exported)
	return string(b), nil
}

func samePath(left, right string) bool {
	left, leftErr := filepath.EvalSymlinks(left)
	right, rightErr := filepath.EvalSymlinks(right)
	return leftErr == nil && rightErr == nil && left == right
}

func validAttemptGitDir(worktree, gitDir, root string) bool {
	dotGit := filepath.Join(worktree, ".git")
	info, err := os.Lstat(dotGit)
	if err != nil || !belowRoot(gitDir, root) {
		return false
	}
	if info.IsDir() {
		return samePath(dotGit, gitDir)
	}
	if !info.Mode().IsRegular() || info.Size() > 4096 {
		return false
	}
	body, err := os.ReadFile(dotGit)
	declared := strings.TrimSpace(strings.TrimPrefix(string(body), "gitdir:"))
	if err != nil || !strings.HasPrefix(string(body), "gitdir:") || declared == "" {
		return false
	}
	if !filepath.IsAbs(declared) {
		declared = filepath.Join(worktree, declared)
	}
	backlink, err := os.ReadFile(filepath.Join(gitDir, "gitdir"))
	linked := strings.TrimSpace(string(backlink))
	if err != nil || len(backlink) > 4096 || linked == "" {
		return false
	}
	if !filepath.IsAbs(linked) {
		linked = filepath.Join(gitDir, linked)
	}
	return samePath(declared, gitDir) && samePath(linked, dotGit)
}

func parseWorkerResult(body []byte) (workerResult, error) {
	var result workerResult
	rd := json.NewDecoder(bytes.NewReader(body))
	rd.DisallowUnknownFields()
	if rd.Decode(&result) != nil || rd.Decode(&struct{}{}) != io.EOF || result.Type != "agent-symphony-result-v1" || strings.TrimSpace(result.Validation) == "" || strings.TrimSpace(result.Documentation) == "" {
		return workerResult{}, errors.New("worker result is invalid")
	}
	return result, nil
}

func readWorkerResult(path string) (workerResult, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 || info.Size() > agentruntime.WorkerResultMaxBytes {
		return workerResult{}, errors.New("worker result is invalid")
	}
	f, err := os.Open(path)
	if err != nil {
		return workerResult{}, errors.New("worker result is invalid")
	}
	opened, statErr := f.Stat()
	body, readErr := io.ReadAll(io.LimitReader(f, agentruntime.WorkerResultMaxBytes+1))
	final, finalStatErr := f.Stat()
	closeErr := f.Close()
	if statErr != nil || finalStatErr != nil || !opened.Mode().IsRegular() || !final.Mode().IsRegular() || !os.SameFile(info, opened) || !os.SameFile(opened, final) || opened.Size() != final.Size() || !opened.ModTime().Equal(final.ModTime()) || readErr != nil || closeErr != nil || len(body) > agentruntime.WorkerResultMaxBytes {
		return workerResult{}, errors.New("worker result is invalid")
	}
	result, err := parseWorkerResult([]byte(internalgithub.RedactEnvironment(string(body), os.Environ())))
	if err != nil {
		return workerResult{}, err
	}
	return result, nil
}

func decodeHandoffRequest(input []byte, root string) (handoffRequest, struct{ Type, Key string }, error) {
	var request handoffRequest
	d := json.NewDecoder(bytes.NewReader(input))
	d.DisallowUnknownFields()
	if d.Decode(&request) != nil || d.Decode(&struct{}{}) != io.EOF || !belowRoot(request.Manifest.Worktree, root) || request.OutcomeToken == "" || len(request.Command) == 0 {
		return request, struct{ Type, Key string }{}, errors.New("invalid handoff request")
	}
	var h struct{ Type, Key string }
	if json.Unmarshal(request.Handoff, &h) != nil || h.Type != "agent-symphony-handoff-v1" || h.Key == "" || filepath.Base(h.Key) != h.Key || strings.ContainsAny(h.Key, "/\\\x00\r\n") {
		return request, h, errors.New("invalid handoff identity")
	}
	if request.OutcomePath != handoffReceiptPath(request.Manifest.Worktree, h.Key) || !belowRoot(request.OutcomePath, request.Manifest.Worktree) {
		return request, h, errors.New("invalid handoff receipt path")
	}
	return request, h, nil
}

func handoffBinding(request handoffRequest) ([]byte, string) {
	binding, _ := json.Marshal(struct {
		State                      string
		Worktree, Session, LogPath string
		Handoff                    json.RawMessage
		OutcomePath, OutcomeToken  string
		Command                    []string
	}{"pending", request.Manifest.Worktree, request.Manifest.Session, request.Manifest.LogPath, request.Handoff, request.OutcomePath, request.OutcomeToken, request.Command})
	return binding, fmt.Sprintf("%x", sha256.Sum256(binding))
}

func verifyHandoff(ctx context.Context, input []byte, root string) (string, error) {
	request, h, err := decodeHandoffRequest(input, root)
	if err != nil {
		return "", err
	}
	binding, recipient := handoffBinding(request)
	persisted, err := immutableMarkerMatches(filepath.Join(request.Manifest.Worktree, ".agent-symphony", "handoffs", h.Key+".json"), binding)
	if err != nil {
		return "", fmt.Errorf("verify handoff binding: %w", err)
	}
	if !persisted {
		return "", nil
	}
	option := "@agent-symphony-handoff-" + recipient[:16]
	observed, err := runHostTmux(ctx, []string{"show-options", "-pqv", "-t", agentruntime.PaneTarget(request.Manifest.Session), option}, nil)
	if err != nil {
		return "", fmt.Errorf("verify handoff launch identity: %w", err)
	}
	if strings.TrimSpace(observed.Output) != recipient {
		return "", nil
	}
	ack, _ := json.Marshal(handoffReceipt{"agent-symphony-handoff-executed-v1", h.Key, request.OutcomePath, request.OutcomeToken})
	return string(ack), nil
}

func acceptOperatorHandoff(ctx context.Context, input []byte, root string) (string, error) {
	request, h, err := decodeHandoffRequest(input, root)
	if err != nil {
		return "", err
	}
	var operator struct{ Kind string }
	if json.Unmarshal(request.Handoff, &operator) != nil || operator.Kind != "operator-message" {
		return "", errors.New("invalid operator handoff")
	}
	inbox := filepath.Join(request.Manifest.Worktree, ".agent-symphony", "handoffs")
	binding, recipient := handoffBinding(request)
	persisted, err := immutableMarkerMatches(filepath.Join(inbox, h.Key+".json"), binding)
	if err != nil {
		return "", fmt.Errorf("operator handoff binding changed: %w", err)
	}
	if !persisted {
		for _, suffix := range []string{".receipt", ".launching", ".launched"} {
			if err := os.RemoveAll(filepath.Join(inbox, h.Key+suffix)); err != nil {
				return "", err
			}
		}
		pane := agentruntime.PaneTarget(request.Manifest.Session)
		option := "@agent-symphony-handoff-" + recipient[:16]
		if _, err := runHostTmux(ctx, []string{"set-option", "-pqu", "-t", pane, option}, nil); err != nil {
			return "", fmt.Errorf("clear stale operator handoff launch identity: %w", err)
		}
	}
	return acceptHandoff(ctx, input, root)
}

func acceptHandoff(ctx context.Context, input []byte, root string) (string, error) {
	request, h, err := decodeHandoffRequest(input, root)
	if err != nil {
		return "", err
	}
	inbox := filepath.Join(request.Manifest.Worktree, ".agent-symphony", "handoffs")
	if err := os.MkdirAll(inbox, 0o700); err != nil {
		return "", err
	}
	binding, recipient := handoffBinding(request)
	if err := writeImmutable(filepath.Join(inbox, h.Key+".json"), binding); err != nil {
		return "", err
	}
	ack, _ := json.Marshal(handoffReceipt{"agent-symphony-handoff-executed-v1", h.Key, request.OutcomePath, request.OutcomeToken})
	if body, err := os.ReadFile(request.OutcomePath); err == nil && bytes.Equal(body, ack) {
		return string(ack), nil
	} else if err == nil || !errors.Is(err, os.ErrNotExist) {
		return "", errors.New("handoff receipt binding mismatch")
	}
	buffer := "as-handoff-" + fmt.Sprintf("%x", sha256.Sum256(request.Handoff))[:16]
	pane := agentruntime.PaneTarget(request.Manifest.Session)
	option := "@agent-symphony-handoff-" + recipient[:16]
	observed, err := runHostTmux(ctx, []string{"show-options", "-pqv", "-t", pane, option}, nil)
	if err == nil && strings.TrimSpace(observed.Output) == recipient {
		if err := writeImmutable(request.OutcomePath, ack); err != nil {
			return "", err
		}
		return string(ack), nil
	}
	resultPath := agentruntime.ResultPath(request.Manifest.Worktree)
	if !belowRoot(resultPath, root) {
		return "", errors.New("handoff result path escapes provisioned root")
	}
	if info, err := os.Lstat(resultPath); err == nil && (!info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0) {
		return "", errors.New("handoff result is not a regular non-symlink file")
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	helper, err := os.Executable()
	if err != nil {
		return "", err
	}
	launchingPath := filepath.Join(inbox, h.Key+".launching")
	launchedPath := filepath.Join(inbox, h.Key+".launched")
	launched, err := immutableMarkerMatches(launchedPath, []byte(recipient))
	if err != nil {
		return "", err
	}
	if launched {
		if err := writeImmutable(request.OutcomePath, ack); err != nil {
			return "", err
		}
		return string(ack), nil
	}
	launching, err := immutableMarkerMatches(launchingPath, []byte(recipient))
	if err != nil {
		return "", err
	}
	if launching {
		state, stateErr := runHostTmux(ctx, []string{"display-message", "-p", "-t", pane, "#{pane_dead}"}, nil)
		if stateErr == nil && strings.TrimSpace(state.Output) == "0" {
			started, startErr := runHostTmux(ctx, []string{"display-message", "-p", "-t", pane, "#{pane_start_command}"}, nil)
			if startErr != nil {
				return "", errors.New("cannot reconcile in-flight handoff launch")
			}
			if strings.Contains(started.Output, " worker-capture-handoff-ready ") && strings.Contains(started.Output, launchedPath) && strings.Contains(started.Output, recipient) {
				return "", errors.New("handoff launch remains in flight")
			}
		}
		if stateErr != nil || strings.TrimSpace(state.Output) != "1" {
			if stateErr != nil || strings.TrimSpace(state.Output) != "0" {
				return "", errors.New("cannot reconcile in-flight handoff launch")
			}
		}
		if err := os.Remove(launchingPath); err != nil {
			return "", err
		}
		if err := immutableDirSync(inbox); err != nil {
			return "", err
		}
	}
	signal := buffer + "-launched"
	command := agentruntime.HandoffPromptCommand(helper, "tmux", buffer, resultPath, launchedPath, recipient, signal, request.Command)
	prompt := fmt.Appendf(nil, "Apply this authorized Agent Symphony handoff in the current worktree. It may contain review feedback or confirmed human instructions. %s Current source refs are available under refs/remotes/agent-symphony/. Do not push; Agent Symphony will publish the captured result.\n\n%s\n\nCompletion contract: Make stdout exactly one JSON line of at most 64 KiB with nonempty validation and documentation evidence; progress and diagnostics belong on stderr. Do not wrap it in Markdown fences or emit another stdout object.\n{\"type\":\"agent-symphony-result-v1\",\"validation\":\"tests run and results\",\"documentation\":\"documentation impact or none\"}", humanInstructionPrecedence, request.Handoff)
	if _, err := runHostTmux(ctx, []string{"load-buffer", "-b", buffer, "-"}, bytes.NewReader(prompt)); err != nil {
		return "", err
	}
	if err := writeImmutable(launchingPath, []byte(recipient)); err != nil {
		return "", err
	}
	tmuxArgs := append(append([]string{"respawn-pane", "-k", "-t", pane, "-c", request.Manifest.Worktree, "--"}, command...), ";", "wait-for", signal)
	if _, err := runHostTmux(ctx, tmuxArgs, nil); err != nil {
		return "", err
	}
	launched, err = immutableMarkerMatches(launchedPath, []byte(recipient))
	if err != nil || !launched {
		return "", errors.New("replacement worker did not produce startup output")
	}
	if _, err := runHostTmux(ctx, []string{"set-option", "-p", "-t", pane, option, recipient}, nil); err != nil {
		return "", err
	}
	if err := writeImmutable(request.OutcomePath, ack); err != nil {
		return "", err
	}
	return string(ack), nil
}

func immutableMarkerMatches(path string, want []byte) (bool, error) {
	body, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil || !bytes.Equal(body, want) {
		return false, errors.New("handoff launch binding mismatch")
	}
	return true, nil
}

func handoffReceiptPath(worktree, key string) string {
	return filepath.Join(worktree, ".agent-symphony", "handoffs", key+".receipt")
}

func belowRoot(path, root string) bool {
	clean, err := filepath.Abs(path)
	if err != nil {
		return false
	}
	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return false
	}
	resolved, err := filepath.EvalSymlinks(clean)
	if err != nil {
		ancestor := clean
		var suffix []string
		for {
			parent := filepath.Dir(ancestor)
			if parent == ancestor {
				return false
			}
			suffix = append(suffix, filepath.Base(ancestor))
			ancestor = parent
			if resolved, err = filepath.EvalSymlinks(ancestor); err == nil {
				break
			}
		}
		for i := len(suffix) - 1; i >= 0; i-- {
			resolved = filepath.Join(resolved, suffix[i])
		}
	}
	rel, err := filepath.Rel(resolvedRoot, resolved)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator)) && !filepath.IsAbs(rel)
}

func hostDiagnostic(stateRoot string) diagnostic {
	if !hostIsolationInstalled() {
		return localHostDiagnostic(stateRoot)
	}
	binary, err := hostExecutable()
	if err != nil {
		return diagnostic{"host isolation", "fail", err.Error(), "Install the current release with install-host."}
	}
	binary, err = filepath.EvalSymlinks(binary)
	if err != nil || validateInstalledBinary(binary) != nil {
		return diagnostic{"host isolation", "fail", "current binary is not the installed root-owned mode-0755 release", "Run doctor from /usr/local/libexec/agent-symphony/<version>/agent-symphony."}
	}
	current, currentErr := hostCurrentUser()
	sudoList, sudoErr := hostOutput("sudo", "-n", "-l")
	if currentErr != nil || sudoErr != nil || !exactSudoAuthority(sudoList, binary) {
		return diagnostic{"host isolation", "fail", "effective sudo authority is missing, stale, or broader than the two managed tuples", "Remove unmanaged grants and rerun the current install-host."}
	}
	for _, pair := range [][2]string{{workerUser, attemptGroup}, {reviewerUser, snapshotGroup}} {
		if err := validateIdentity(pair[0], pair[1]); err != nil {
			return diagnostic{"host isolation", "fail", err.Error(), "Repair conflicting host identities, then rerun install-host."}
		}
	}
	base := "/var/lib/agent-symphony"
	if hostGOOS == "darwin" {
		base = "/var/db/agent-symphony"
	}
	worker, _ := hostLookupUser(workerUser)
	attempt, _ := hostLookupGroup(attemptGroup)
	coordinator, coordinatorErr := hostLookupUser(current.Username)
	snapshot, _ := hostLookupGroup(snapshotGroup)
	if coordinatorErr != nil {
		return diagnostic{"host isolation", "fail", "managed coordinator identity is missing", "Rerun install-host with the intended coordinator."}
	}
	groupOutput, groupErr := hostOutput("id", "-G", coordinator.Username)
	memberships := map[string]bool{}
	for _, gid := range strings.Fields(string(groupOutput)) {
		memberships[gid] = true
	}
	if groupErr != nil || !memberships[attempt.Gid] || !memberships[snapshot.Gid] {
		return diagnostic{"host isolation", "fail", "coordinator is missing required supplementary groups", "Rerun install-host, then start a new login session."}
	}
	for _, root := range []struct {
		path, uid, gid string
		mode           os.FileMode
	}{{base + "/attempts", worker.Uid, attempt.Gid, os.ModeSetgid | 0o770}, {base + "/snapshots", coordinator.Uid, snapshot.Gid, 0o750}} {
		info, statErr := os.Stat(root.path)
		if statErr != nil || !info.IsDir() || info.Mode()&(os.ModePerm|os.ModeSetgid) != root.mode || fileUID(info) != atoi(root.uid) || fileGID(info) != atoi(root.gid) {
			return diagnostic{"host isolation", "fail", root.path + " ownership or mode is unsafe", "Rerun install-host after repairing conflicting state."}
		}
	}
	stateCanary, err := os.MkdirTemp("", "agent-symphony-doctor-state-")
	if err != nil {
		return diagnostic{"host isolation", "fail", "cannot create coordinator denial canaries", err.Error()}
	}
	defer os.RemoveAll(stateCanary)
	secret := filepath.Join(stateCanary, "secret")
	socket := filepath.Join(stateCanary, "control.sock")
	if os.WriteFile(secret, []byte("secret-canary\n"), 0o600) != nil || os.WriteFile(socket, nil, 0o600) != nil {
		return diagnostic{"host isolation", "fail", "cannot create coordinator denial canaries", "Repair the coordinator temporary directory."}
	}
	snapshotCanaryDir, err := os.MkdirTemp(base+"/snapshots", ".doctor-snapshot-")
	if err != nil {
		return diagnostic{"host isolation", "fail", "cannot create reviewer snapshot canary", "Repair snapshot root ownership."}
	}
	defer func() { _ = os.Chmod(snapshotCanaryDir, 0o750); _ = os.RemoveAll(snapshotCanaryDir) }()
	snapshotCanary := filepath.Join(snapshotCanaryDir, "snapshot")
	if err := os.WriteFile(snapshotCanary, []byte("agent-symphony-review-canary\n"), 0o440); err != nil || os.Chown(snapshotCanaryDir, -1, atoi(snapshot.Gid)) != nil || os.Chown(snapshotCanary, -1, atoi(snapshot.Gid)) != nil || os.Chmod(snapshotCanaryDir, 0o550) != nil {
		return diagnostic{"host isolation", "fail", "cannot seal reviewer snapshot canary", "Repair snapshot group ownership."}
	}
	canary, _ := json.Marshal(struct {
		Deny     []string `json:"deny"`
		Snapshot string   `json:"snapshot"`
	}{[]string{stateCanary, secret, socket}, snapshotCanary})
	requestBody, _ := json.Marshal(struct {
		Operation string          `json:"operation"`
		Command   boundaryCommand `json:"command"`
	}{"verify", boundaryCommand{Input: canary}})
	request := requestBody
	for _, probe := range []struct {
		want  bool
		user  string
		group string
		args  []string
	}{
		{true, workerUser, attemptGroup, []string{binary, "agent-host", "implementation"}},
		{true, reviewerUser, snapshotGroup, []string{binary, "agent-host", "review"}},
		{false, workerUser, snapshotGroup, []string{binary, "agent-host", "implementation"}},
		{false, reviewerUser, attemptGroup, []string{binary, "agent-host", "review"}},
		{false, workerUser, attemptGroup, []string{binary, "agent-host", "implementation", "extra"}},
		{false, workerUser, attemptGroup, []string{"/bin/sh"}},
		{false, workerUser, attemptGroup, []string{"/usr/bin/id"}},
		{false, "root", "root", []string{binary, "agent-host", "implementation"}},
	} {
		args := append([]string{"-n", "-u", probe.user, "-g", probe.group}, probe.args...)
		err := hostProbe("sudo", args, request)
		if (err == nil) != probe.want {
			return diagnostic{"host isolation", "fail", "sudo allow/deny or worker access canary failed", "Remove broader sudo grants, repair root access, and rerun install-host."}
		}
	}
	return diagnostic{"host isolation", "pass", "current binary identities and exact managed sudo rules are installed", ""}
}

// localHostDiagnostic validates the zero-admin default boundary: a private
// local root the coordinator can create and write to. It intentionally does
// not attempt to prove OS-enforced isolation between the coordinator and the
// agent process, because none exists in this mode — see docs/security.md.
func localHostDiagnostic(stateRoot string) diagnostic {
	if strings.TrimSpace(stateRoot) == "" {
		return diagnostic{"host isolation", "fail", "runtime state root is required to provision the local attempt/snapshot roots", "Pass --runtime-state, or run install-host for the advanced host-isolated path."}
	}
	for _, root := range []string{localAttemptRoot(stateRoot), localSnapshotRoot(stateRoot)} {
		if err := verifyLocalAccess(root); err != nil {
			return diagnostic{"host isolation", "fail", err.Error(), "Repair " + root + " ownership and mode, or run install-host for the advanced host-isolated path."}
		}
	}
	return diagnostic{"host isolation", "pass", "zero-admin default boundary is active: no separate OS identity, reduced isolation from the agent process", "Run install-host for OS-enforced isolation between the coordinator and the agent."}
}

func fileUID(info os.FileInfo) int {
	if stat, ok := info.Sys().(*syscall.Stat_t); ok {
		return int(stat.Uid)
	}
	return -1
}

func fileGID(info os.FileInfo) int {
	if stat, ok := info.Sys().(*syscall.Stat_t); ok {
		return int(stat.Gid)
	}
	return -1
}
func atoi(value string) int { n, _ := strconv.Atoi(value); return n }
