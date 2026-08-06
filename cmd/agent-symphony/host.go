package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
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
	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

const (
	workerUser    = "agent-symphony-worker"
	reviewerUser  = "agent-symphony-reviewer"
	attemptGroup  = "agent-symphony-attempt"
	snapshotGroup = "agent-symphony-snapshot"
)

var (
	hostGOOS        = runtime.GOOS
	hostEUID        = os.Geteuid
	hostEGID        = os.Getegid
	hostExecutable  = os.Executable
	hostLookupUser  = user.Lookup
	hostLookupGroup = user.LookupGroup
	hostCurrentUser = user.Current
	hostRun         = func(name string, args ...string) error {
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
			return exactSudoAuthority(body, binary)
		}
	}
	return true
}

func exactSudoAuthority(body []byte, binary string) bool {
	want := map[string]bool{
		workerUser + ":" + attemptGroup + "\x00" + binary + " agent-host implementation": false,
		reviewerUser + ":" + snapshotGroup + "\x00" + binary + " agent-host review":      false,
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
	return want[workerUser+":"+attemptGroup+"\x00"+binary+" agent-host implementation"] && want[reviewerUser+":"+snapshotGroup+"\x00"+binary+" agent-host review"]
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
	body := fmt.Sprintf("# managed by agent-symphony; rerun install-host after upgrades\n%s ALL=(%s:%s) NOPASSWD: %s agent-host implementation\n%s ALL=(%s:%s) NOPASSWD: %s agent-host review\n", coordinator, workerUser, attemptGroup, binary, coordinator, reviewerUser, snapshotGroup, binary)
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

type reviewResultRequest struct {
	Repository string `json:"repository"`
	Issue      int    `json:"issue"`
	Attempt    int    `json:"attempt"`
	Head       string `json:"head"`
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
	snapshot, _ := reviewIdentity(agentruntime.Attempt{Repository: request.Repository, Issue: request.Issue, Number: request.Attempt}, root)
	path := reviewResultPath(snapshot, request.Head)
	if !belowRoot(path, root) {
		return "", errors.New("review result path escapes snapshot root")
	}
	listed, err := os.Lstat(path)
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
	return string(body), nil
}

func agentHost(ctx context.Context, mode string, input io.Reader, output io.Writer) error {
	localRoot := strings.TrimSpace(os.Getenv("AGENT_SYMPHONY_LOCAL_ROOT"))
	wantUser, wantGroup, root := workerUser, attemptGroup, "/var/lib/agent-symphony/attempts"
	if hostGOOS == "darwin" {
		root = "/var/db/agent-symphony/attempts"
	}
	if mode == "review" {
		wantUser, wantGroup = reviewerUser, snapshotGroup
		root = strings.Replace(root, "attempts", "snapshots", 1)
	} else if mode != "implementation" {
		return errors.New("agent-host mode must be implementation or review")
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
		result, err = hostExecRunner(ctx, agentruntime.Command{Name: request.Command.Name, Args: request.Command.Args, Dir: request.Command.Dir, Env: env, Stdin: bytes.NewReader(request.Command.Input)})
	case "export":
		if mode != "implementation" {
			return errors.New("review boundary cannot export implementation attempts")
		}
		result.Output, err = exportAttempt(ctx, request.Command.Input, root)
	case "accept-handoff":
		if mode != "implementation" {
			return errors.New("review boundary cannot accept implementation handoffs")
		}
		result.Output, err = acceptHandoff(ctx, request.Command.Input, root)
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
	if err != nil && !result.Exited {
		return err
	}
	return json.NewEncoder(output).Encode(result)
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
	if (c.Name == "git" && !validGitBoundaryArgs(c.Args, c.Dir, root)) || (c.Name == "tmux" && !validTmuxBoundaryArgs(c.Args, c.Dir, root)) {
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
		(len(rest) == 3 && rest[0] == "checkout" && rest[1] == "--detach") ||
		(len(rest) == 3 && rest[0] == "switch" && rest[1] == "-c") ||
		slices.Equal(rest, []string{"remote", "remove", "origin"}) ||
		slices.Equal(rest, []string{"config", "--local", "credential.helper", ""})
}

func validTmuxBoundaryArgs(args []string, dir, root string) bool {
	if len(args) == 0 || strings.HasPrefix(args[0], "-") {
		return false
	}
	switch args[0] {
	case "new-session":
		if len(args) < 6 || args[1] != "-d" || args[2] != "-s" || args[4] != "-c" || !boundedCommandPath(args[5], dir, root) {
			return false
		}
		for i := 6; i < len(args); i += 2 {
			if i+1 >= len(args) || args[i] != "-e" || !strings.Contains(args[i+1], "=") {
				return false
			}
		}
		return true
	case "has-session", "kill-session":
		return len(args) == 3 && args[1] == "-t" && validTmuxTarget(args[2], false)
	case "display-message":
		return len(args) == 5 && args[1] == "-p" && args[2] == "-t" && validTmuxTarget(args[3], true) && (args[4] == "#{pane_dead}" || args[4] == "#{pane_dead} #{pane_dead_status}")
	case "capture-pane":
		return len(args) == 6 && slices.Equal(args[1:5], []string{"-p", "-S", "-", "-t"}) && validTmuxTarget(args[5], true)
	case "set-option":
		return len(args) == 6 && slices.Equal(args[1:3], []string{"-w", "-t"}) && validTmuxTarget(args[3], true) && ((args[4] == "remain-on-exit" && args[5] == "on") || (args[4] == "history-limit" && args[5] == "5000"))
	case "respawn-pane":
		return len(args) > 5 && slices.Equal(args[1:3], []string{"-k", "-t"}) && validTmuxTarget(args[3], true) && args[4] == "--" && args[5] != ""
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

func validTmuxTarget(target string, pane bool) bool {
	if !strings.HasPrefix(target, "=") || strings.ContainsAny(target, "/\\\x00\r\n") {
		return false
	}
	return !pane || strings.HasSuffix(target, ":0.0")
}

func reservedHostEnvironment(name string) bool {
	upper := strings.ToUpper(name)
	if upper == "HOME" {
		return true
	}
	for _, part := range []string{"TOKEN", "SECRET", "PASSWORD", "PASSWD", "PRIVATE_KEY", "PRIVATE-KEY", "CREDENTIAL", "AUTHORIZATION", "GITHUB_PAT", "WEBHOOK"} {
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
	status, err := run("status", "--porcelain")
	if err != nil {
		return "", errors.New("inspect export worktree")
	}
	if status != "" {
		if _, err := run("add", "--all"); err != nil {
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
	if status, err := run("status", "--porcelain"); err != nil || status != "" {
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
	result, err := parseWorkerResult(body)
	if err != nil {
		return workerResult{}, err
	}
	return result, nil
}

func acceptHandoff(ctx context.Context, input []byte, root string) (string, error) {
	var request struct {
		Manifest     agentruntime.Manifest `json:"manifest"`
		Handoff      json.RawMessage       `json:"handoff"`
		OutcomePath  string                `json:"outcome_path"`
		OutcomeToken string                `json:"outcome_token"`
		Command      []string              `json:"command"`
	}
	d := json.NewDecoder(bytes.NewReader(input))
	d.DisallowUnknownFields()
	if d.Decode(&request) != nil || d.Decode(&struct{}{}) != io.EOF || !belowRoot(request.Manifest.Worktree, root) || request.OutcomePath != request.Manifest.LogPath+".review-outcome" || !belowRoot(request.OutcomePath, request.Manifest.Worktree) || request.OutcomeToken == "" || len(request.Command) == 0 {
		return "", errors.New("invalid handoff request")
	}
	var h struct{ Type, Key string }
	if json.Unmarshal(request.Handoff, &h) != nil || h.Type != "agent-symphony-handoff-v1" || h.Key == "" || filepath.Base(h.Key) != h.Key || strings.ContainsAny(h.Key, "/\\\x00\r\n") {
		return "", errors.New("invalid handoff identity")
	}
	inbox := filepath.Join(request.Manifest.Worktree, ".agent-symphony", "handoffs")
	if err := os.MkdirAll(inbox, 0o700); err != nil {
		return "", err
	}
	binding, _ := json.Marshal(struct {
		State                      string
		Worktree, Session, LogPath string
		Handoff                    json.RawMessage
		OutcomePath, OutcomeToken  string
		Command                    []string
	}{"pending", request.Manifest.Worktree, request.Manifest.Session, request.Manifest.LogPath, request.Handoff, request.OutcomePath, request.OutcomeToken, request.Command})
	if err := writeImmutable(filepath.Join(inbox, h.Key+".json"), binding); err != nil {
		return "", err
	}
	ack, _ := json.Marshal(struct{ Type, Key, OutcomePath, OutcomeToken string }{"agent-symphony-handoff-executed-v1", h.Key, request.OutcomePath, request.OutcomeToken})
	if body, err := os.ReadFile(request.OutcomePath); err == nil && bytes.Equal(body, ack) {
		return string(ack), nil
	} else if err == nil || !errors.Is(err, os.ErrNotExist) {
		return "", errors.New("handoff receipt binding mismatch")
	}
	buffer := "as-handoff-" + fmt.Sprintf("%x", sha256.Sum256(request.Handoff))[:16]
	pane, recipient := agentruntime.PaneTarget(request.Manifest.Session), fmt.Sprintf("%x", sha256.Sum256(binding))
	option := "@agent-symphony-handoff-" + recipient[:16]
	observed, err := hostExecRunner(ctx, agentruntime.Command{Name: "tmux", Args: []string{"show-options", "-pqv", "-t", pane, option}, Dir: request.Manifest.Worktree})
	if err == nil && strings.TrimSpace(observed.Output) == recipient {
		if err := writeImmutable(request.OutcomePath, ack); err != nil {
			return "", err
		}
		return string(ack), nil
	}
	commands := []agentruntime.Command{
		{Name: "tmux", Args: []string{"load-buffer", "-b", buffer, "-"}, Dir: request.Manifest.Worktree, Stdin: bytes.NewReader(request.Handoff)},
		{Name: "tmux", Args: append(append([]string{"respawn-pane", "-k", "-t", pane, "--"}, request.Command...), ";", "paste-buffer", "-d", "-b", buffer, "-t", pane, ";", "send-keys", "-t", pane, "Enter", ";", "set-option", "-p", "-t", pane, option, recipient), Dir: request.Manifest.Worktree},
	}
	for _, command := range commands {
		if _, err := hostExecRunner(ctx, command); err != nil {
			return "", err
		}
	}
	if err := writeImmutable(request.OutcomePath, ack); err != nil {
		return "", err
	}
	return string(ack), nil
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
