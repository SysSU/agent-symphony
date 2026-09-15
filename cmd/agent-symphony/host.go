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
	"net"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/SysSU/agent-symphony/internal/config"
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
	hostOutput           = func(name string, args ...string) ([]byte, error) { return exec.Command(name, args...).Output() }
	hostExecRunner       = (agentruntime.ExecRunner{}).Run
	hostReviewResultOpen = syscall.Open
	hostRoot             = ""
	sandboxExecutable    = os.Executable
	rootlessCodexVerify  = verifyRootlessCodex
)

func runHostTmux(ctx context.Context, args []string, stdin io.Reader) (agentruntime.Result, error) {
	return hostExecRunner(ctx, agentruntime.Command{Name: "tmux", Args: args, Dir: "/tmp", Stdin: stdin})
}

func nativeRoot(path string) string { return filepath.Join(hostRoot, path) }

// hostIsolationInstalled reports legacy identities for migration diagnostics.
// Their presence never changes the rootless runtime boundary.
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

type codexConfinementProof struct {
	Confined        bool `json:"confined"`
	SharedTempRead  bool `json:"shared_temp_read"`
	SharedTempWrite bool `json:"shared_temp_write"`
}

func verifyRootlessCodex(ctx context.Context, root, codexHome, codexExecutable string) (codexConfinementProof, error) {
	if err := verifyLocalAccess(root); err != nil {
		return codexConfinementProof{}, err
	}
	if info, err := os.Lstat(codexHome); err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o077 != 0 {
		return codexConfinementProof{}, errors.New("isolated worker CODEX_HOME is unavailable or unsafe")
	}
	workspace, err := os.MkdirTemp(root, ".sandbox-preflight-")
	if err != nil {
		return codexConfinementProof{}, err
	}
	defer os.RemoveAll(workspace)
	private := filepath.Join(workspace, ".agent-symphony")
	if err := os.Mkdir(private, 0o700); err != nil {
		return codexConfinementProof{}, err
	}
	for _, name := range []string{".codex", ".agents"} {
		directory := filepath.Join(workspace, name)
		if err := os.Mkdir(directory, 0o700); err != nil {
			return codexConfinementProof{}, err
		}
		if err := os.WriteFile(filepath.Join(directory, "deny"), []byte("deny\n"), 0o600); err != nil {
			return codexConfinementProof{}, err
		}
	}
	otherAttempt, err := os.MkdirTemp(root, ".sandbox-other-attempt-")
	if err != nil {
		return codexConfinementProof{}, err
	}
	defer os.RemoveAll(otherAttempt)
	canaryPath := filepath.Join(otherAttempt, "deny")
	canary, err := os.OpenFile(canaryPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return codexConfinementProof{}, err
	}
	if canary.Chmod(0o600) != nil || func() error { _, err := canary.WriteString("deny\n"); return err }() != nil || canary.Close() != nil {
		return codexConfinementProof{}, errors.New("prepare sandbox deny canary")
	}
	stateCanary := filepath.Join(filepath.Dir(root), fmt.Sprintf(".sandbox-state-%d", os.Getpid()))
	if err := os.WriteFile(stateCanary, []byte("deny\n"), 0o600); err != nil {
		return codexConfinementProof{}, err
	}
	defer os.Remove(stateCanary)
	authCanary := filepath.Join(codexHome, ".sandbox-auth-link")
	if err := os.Symlink(stateCanary, authCanary); err != nil {
		return codexConfinementProof{}, err
	}
	defer os.Remove(authCanary)
	tcpListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return codexConfinementProof{}, err
	}
	defer tcpListener.Close()
	unixPath := filepath.Join(root, fmt.Sprintf(".sandbox-%d.sock", os.Getpid()))
	unixListener, err := net.Listen("unix", unixPath)
	if err != nil {
		return codexConfinementProof{}, err
	}
	defer func() { unixListener.Close(); _ = os.Remove(unixPath) }()
	sharedTemp, err := os.CreateTemp("/tmp", ".agent-symphony-sandbox-")
	if err != nil {
		return codexConfinementProof{}, err
	}
	sharedTempPath := sharedTemp.Name()
	if _, err := sharedTemp.WriteString("deny\n"); err != nil {
		sharedTemp.Close()
		return codexConfinementProof{}, err
	}
	if err := sharedTemp.Close(); err != nil {
		return codexConfinementProof{}, err
	}
	defer os.Remove(sharedTempPath)
	executable, err := sandboxExecutable()
	if err != nil {
		return codexConfinementProof{}, err
	}
	probe := filepath.Join(workspace, "probe")
	input, err := os.Open(executable)
	if err != nil {
		return codexConfinementProof{}, err
	}
	output, err := os.OpenFile(probe, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o700)
	if err != nil {
		input.Close()
		return codexConfinementProof{}, err
	}
	_, copyErr := io.Copy(output, input)
	err = errors.Join(copyErr, output.Sync(), output.Close(), input.Close())
	if err != nil {
		return codexConfinementProof{}, err
	}
	proof := filepath.Join(workspace, "proof")
	args := config.WorkerSandboxArgsForExecutable(workspace, codexExecutable, probe, "sandbox-probe", proof, canaryPath, stateCanary, authCanary, tcpListener.Addr().String(), unixPath, sharedTempPath)
	command := exec.CommandContext(ctx, codexExecutable, args...)
	command.Env = []string{"PATH=" + os.Getenv("PATH"), "CODEX_HOME=" + codexHome, "TMPDIR=" + filepath.Join(private, "tmp")}
	if err := os.Mkdir(filepath.Join(private, "tmp"), 0o700); err != nil {
		return codexConfinementProof{}, err
	}
	if out, err := command.CombinedOutput(); err != nil {
		return codexConfinementProof{}, fmt.Errorf("rootless Codex confinement preflight failed: %w: %s", err, strings.TrimSpace(string(out)))
	}
	body, err := os.ReadFile(proof)
	var result codexConfinementProof
	if err != nil || json.Unmarshal(body, &result) != nil || !result.Confined {
		return codexConfinementProof{}, errors.New("rootless Codex confinement preflight produced no proof")
	}
	return result, nil
}

func runSandboxProbe(args []string, child bool) error {
	if len(args) != 7 && len(args) != 9 {
		return errors.New("invalid sandbox probe")
	}
	if !child {
		command := exec.Command(os.Args[0], append([]string{"sandbox-probe-child"}, args...)...)
		command.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
		if output, err := command.CombinedOutput(); err != nil {
			return fmt.Errorf("detached sandbox probe failed: %w: %s", err, strings.TrimSpace(string(output)))
		}
		return nil
	}
	if len(args) == 9 {
		ready, err := os.OpenFile(args[7], os.O_WRONLY, 0)
		if err != nil {
			return err
		}
		_, writeErr := ready.Write([]byte{1})
		closeErr := ready.Close()
		if writeErr != nil || closeErr != nil {
			return errors.Join(writeErr, closeErr)
		}
		release, err := os.Open(args[8])
		if err != nil {
			return err
		}
		var signal [1]byte
		_, readErr := io.ReadFull(release, signal[:])
		closeErr = release.Close()
		if readErr != nil || closeErr != nil {
			return errors.Join(readErr, closeErr)
		}
	}
	if _, err := os.ReadFile(args[1]); err == nil {
		return errors.New("sandbox read sibling canary")
	}
	if err := os.WriteFile(args[1], []byte("mutated"), 0o600); err == nil {
		return errors.New("sandbox wrote sibling canary")
	}
	if _, err := os.ReadFile(args[2]); err == nil {
		return errors.New("sandbox read coordinator state canary")
	}
	if _, err := os.ReadFile(args[3]); err == nil {
		return errors.New("sandbox read worker credential canary")
	}
	wantTemp := filepath.Join(filepath.Dir(args[0]), ".agent-symphony", "tmp")
	if filepath.Clean(os.TempDir()) != wantTemp {
		return errors.New("sandbox did not receive its attempt-private temporary directory")
	}
	_, sharedReadErr := os.ReadFile(args[6])
	sharedWriteErr := os.WriteFile(args[6], []byte("mutated"), 0o600)
	for _, directory := range []string{filepath.Join(filepath.Dir(args[0]), ".codex"), filepath.Join(filepath.Dir(args[0]), ".agents")} {
		path := filepath.Join(directory, "deny")
		if _, err := os.ReadFile(path); err == nil {
			return errors.New("sandbox read denied local agent configuration")
		}
		if err := os.WriteFile(path, []byte("mutated"), 0o600); err == nil {
			return errors.New("sandbox wrote denied local agent configuration")
		}
	}
	if connection, err := net.Dial("tcp", args[4]); err == nil {
		connection.Close()
		return errors.New("sandbox opened network connection")
	}
	if connection, err := net.Dial("unix", args[5]); err == nil {
		connection.Close()
		return errors.New("sandbox opened coordinator socket")
	}
	proof, _ := json.Marshal(codexConfinementProof{Confined: true, SharedTempRead: sharedReadErr == nil, SharedTempWrite: sharedWriteErr == nil})
	if err := os.WriteFile(args[0], proof, 0o600); err != nil {
		return fmt.Errorf("sandbox cannot write disposable workspace: %w", err)
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

func sudoersPolicy(coordinator, binary string) string {
	return fmt.Sprintf("# managed by agent-symphony; rerun install-host after upgrades\nDefaults!%s env_keep += \"%s\"\n%s ALL=(%s:%s) NOPASSWD: %s agent-host implementation\n%s ALL=(%s:%s) NOPASSWD: %s agent-host review\n%s ALL=(%s:%s) NOPASSWD: %s agent-host orchestrator\n", binary, strings.Join(internalgithub.GitHubCLIEnvironmentNames(), " "), coordinator, workerUser, attemptGroup, binary, coordinator, reviewerUser, snapshotGroup, binary, coordinator, reviewerUser, snapshotGroup, binary)
}

type reviewResultRequest struct {
	Repository         string `json:"repository"`
	Issue              int    `json:"issue"`
	Attempt            int    `json:"attempt"`
	Mode               string `json:"mode"`
	Target             string `json:"target"`
	RunID              string `json:"run_id"`
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
	if !agentruntime.ValidReviewMetadata(request.Mode, request.Target) || !validReviewTarget(request.Mode, request.Target, request.Repository, request.Issue, request.Head) || !validDigest(request.RunID) || request.LegacyHeadArtifact && request.Mode != agentruntime.ReviewModeImplementation {
		return "", errors.New("invalid review result request")
	}
	snapshot, _ := reviewRunIdentity(agentruntime.Attempt{Repository: request.Repository, Issue: request.Issue, Number: request.Attempt}, root, request.Target, request.RunID)
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
	var env []string
	if launch.OneShot {
		env, err = internalgithub.WorkerEnvironmentWith(os.Environ())
	} else {
		env, err = internalgithub.AgentEnvironmentWith(os.Environ())
	}
	if err != nil {
		return err
	}
	if launch.OneShot {
		auditHome := dir
		for _, value := range env {
			if strings.HasPrefix(value, "CODEX_HOME=") && strings.TrimPrefix(value, "CODEX_HOME=") != "" {
				auditHome = strings.TrimPrefix(value, "CODEX_HOME=")
				break
			}
		}
		env = append(env, "HOME="+auditHome)
	} else {
		env = append(env, "HOME="+home)
	}
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
		Version               int    `json:"version"`
		Repository            string `json:"repository"`
		Issue                 int    `json:"issue"`
		Attempt               int    `json:"attempt"`
		Action                string `json:"action,omitempty"`
		RequestID             string `json:"request_id,omitempty"`
		HandoffID             string `json:"handoff_id,omitempty"`
		Detail                string `json:"detail,omitempty"`
		IssueGeneration       uint64 `json:"issue_generation,omitempty"`
		AttemptGeneration     uint64 `json:"attempt_generation,omitempty"`
		MachineStatusSequence uint64 `json:"machine_status_sequence,omitempty"`
		OwnerCausalityToken   string `json:"owner_causality_token,omitempty"`
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&proposal) != nil || decoder.Decode(&struct{}{}) != io.EOF || proposal.Version != 1 {
		return orchestratoragent.MessageProposal{}, nil, errors.New("invalid orchestrator proposal schema")
	}
	parsed := orchestratoragent.MessageProposal{Version: proposal.Version, Repository: proposal.Repository, Issue: proposal.Issue, Attempt: proposal.Attempt, Action: proposal.Action, RequestID: proposal.RequestID, HandoffID: proposal.HandoffID, Detail: proposal.Detail, IssueGeneration: proposal.IssueGeneration, AttemptGeneration: proposal.AttemptGeneration, MachineStatusSequence: proposal.MachineStatusSequence, OwnerCausalityToken: proposal.OwnerCausalityToken}
	if err := orchestratoragent.ValidateMessageProposal(parsed); err != nil {
		return orchestratoragent.MessageProposal{}, nil, err
	}
	canonical, _ := json.Marshal(proposal)
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
	// AGENT_SYMPHONY_LOCAL_ROOT is set by the coordinator's rootless boundary;
	// there is no separate OS identity to verify in that mode, only the same
	// user the coordinator itself runs as. The legacy decoder branch is not
	// selected by the production launcher.
	var homeDir string
	var err error
	if localRoot != "" {
		if !filepath.IsAbs(localRoot) || filepath.Clean(localRoot) != localRoot {
			return errors.New("local boundary root must be canonical and absolute")
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
		env, filterErr := boundaryEnvironment(request.Command.Env)
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
		var manifest agentruntime.Manifest
		decoder := json.NewDecoder(bytes.NewReader(request.Command.Input))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&manifest) != nil || decoder.Decode(&struct{}{}) != io.EOF || !belowRoot(manifest.Worktree, root) {
			return errors.New("invalid export manifest")
		}
		binary, binaryErr := hostExecutable()
		if binaryErr != nil {
			return binaryErr
		}
		tmp := filepath.Join(manifest.Worktree, ".agent-symphony", "tmp")
		if err := os.MkdirAll(tmp, 0o700); err != nil {
			return err
		}
		codexExecutable := strings.TrimSpace(os.Getenv("AGENT_SYMPHONY_CODEX_EXECUTABLE"))
		profileDigest := strings.TrimSpace(os.Getenv("AGENT_SYMPHONY_WORKER_PROFILE_DIGEST"))
		if !filepath.IsAbs(codexExecutable) || !validDigest(profileDigest) || manifest.WorkerProfileDigest != profileDigest {
			return errors.New("worker export confinement identity is unavailable")
		}
		if err := config.VerifyWorkerExecutable(ctx, codexExecutable, profileDigest); err != nil {
			return err
		}
		result, err = hostExecRunner(ctx, agentruntime.Command{Name: codexExecutable, Args: config.WorkerSandboxArgsForExecutable(manifest.Worktree, codexExecutable, binary, "export-attempt", root), Dir: manifest.Worktree, Env: []string{"PATH=" + os.Getenv("PATH"), "CODEX_HOME=" + os.Getenv("CODEX_HOME"), "TMPDIR=" + tmp}, Stdin: bytes.NewReader(request.Command.Input)})
	case "validate-cleanup", "cleanup":
		if mode != "implementation" {
			return errors.New("review boundary cannot clean implementation attempts")
		}
		err = validateOrCleanupAttempt(ctx, request.Command.Input, root, request.Operation == "cleanup")
	case "validate-abandon", "abandon":
		if mode != "implementation" {
			return errors.New("review boundary cannot abandon implementation attempts")
		}
		err = validateOrAbandonAttempt(ctx, request.Command.Input, root, request.Operation == "abandon")
	case "validate-remove", "remove":
		if mode != "implementation" {
			return errors.New("review boundary cannot permanently remove implementation attempts")
		}
		err = permanentlyRemoveAttempt(ctx, request.Command.Input, root, request.Operation == "remove")
	case "accept-handoff":
		if mode != "implementation" {
			return errors.New("review boundary cannot accept implementation handoffs")
		}
		result.Output, err = acceptHandoff(ctx, request.Command.Input, root)
	case "prepare-handoff":
		if mode != "implementation" {
			return errors.New("review boundary cannot prepare implementation handoffs")
		}
		result.Output, err = prepareHandoffV2(ctx, request.Command.Input, root)
	case "release-handoff":
		if mode != "implementation" {
			return errors.New("review boundary cannot release implementation handoffs")
		}
		result.Output, err = releaseHandoffV2(ctx, request.Command.Input, root)
	case "compensate-handoff":
		if mode != "implementation" {
			return errors.New("review boundary cannot compensate implementation handoffs")
		}
		result.Output, err = compensateHandoffV2(ctx, request.Command.Input, root)
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
	return validateOrCleanupAttempt(ctx, input, root, true)
}

func abandonAttempt(ctx context.Context, input []byte, root string) error {
	return validateOrAbandonAttempt(ctx, input, root, true)
}

func validateOrCleanupAttempt(ctx context.Context, input []byte, root string, remove bool) error {
	return removeAttemptResources(ctx, input, root, true, remove)
}

func validateOrAbandonAttempt(ctx context.Context, input []byte, root string, remove bool) error {
	return removeAttemptResources(ctx, input, root, false, remove)
}

type permanentRemovalRequest struct {
	Manifest      agentruntime.Manifest `json:"manifest"`
	PublishedHead string                `json:"published_head,omitempty"`
}

func permanentlyRemoveAttempt(ctx context.Context, input []byte, root string, remove bool) error {
	var request permanentRemovalRequest
	decoder := json.NewDecoder(bytes.NewReader(input))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&request) != nil || decoder.Decode(&struct{}{}) != io.EOF || !preflightObjectID.MatchString(request.PublishedHead) {
		return errors.New("invalid permanent removal request")
	}
	return removeVerifiedAttemptResources(ctx, request.Manifest, root, false, request.PublishedHead, remove)
}

func removeAttemptResources(ctx context.Context, input []byte, root string, completed, remove bool) error {
	var manifest agentruntime.Manifest
	decoder := json.NewDecoder(bytes.NewReader(input))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&manifest) != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return errors.New("invalid attempt manifest")
	}
	return removeVerifiedAttemptResources(ctx, manifest, root, completed, "", remove)
}

func removeVerifiedAttemptResources(ctx context.Context, manifest agentruntime.Manifest, root string, completed bool, publishedHead string, remove bool) error {
	attempt := agentruntime.Attempt{Repository: manifest.Repository, Issue: manifest.Issue, Number: manifest.Attempt, BaseSHA: manifest.BaseSHA}
	want, err := agentruntime.AttemptIdentity(root, attempt)
	validState := manifest.State == "preparing" || manifest.State == "running" || manifest.State == "completed" || manifest.State == "failed" || manifest.State == "cancelled"
	if err != nil || !agentruntime.ValidManifestVersion(manifest) || !validState || manifest.Branch != want.Branch || manifest.Worktree != want.Worktree || manifest.Session != want.Session ||
		(completed && (manifest.State != "completed" || !preflightObjectID.MatchString(manifest.ReviewHead))) ||
		(publishedHead != "" && !preflightObjectID.MatchString(publishedHead)) {
		return errors.New("invalid attempt manifest")
	}
	if publishedHead != "" && manifest.State != "completed" && manifest.State != "failed" && manifest.State != "cancelled" {
		return errors.New("permanent removal requires a terminal attempt")
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
		if completed || publishedHead != "" {
			branch, branchErr := run("branch", "--show-current")
			head, headErr := run("rev-parse", "HEAD")
			wantHead := manifest.ReviewHead
			if publishedHead != "" {
				wantHead = publishedHead
			}
			if branchErr != nil || branch != want.Branch || headErr != nil || !strings.EqualFold(head, wantHead) {
				return errors.New("cleanup worktree identity changed")
			}
		}
		if publishedHead != "" {
			status, statusErr := run("status", "--porcelain=v1", "--untracked-files=all", "--", ".", ":(exclude).agent-symphony")
			if statusErr != nil || status != "" {
				return errors.New("permanent removal refused because the worktree has uncommitted changes")
			}
		}
	}

	privatePath := agentruntime.PrivatePath(want.Worktree)
	privateInfo, privateErr := os.Lstat(privatePath)
	if privateErr != nil && !errors.Is(privateErr, os.ErrNotExist) {
		return privateErr
	}
	if privateErr == nil && (!privateInfo.IsDir() || privateInfo.Mode()&os.ModeSymlink != 0 || privateInfo.Mode().Perm()&0o077 != 0) {
		return errors.New("cleanup worker-private path is unsafe")
	}
	if !remove {
		return nil
	}
	if err := stopAttemptSession(ctx, manifest); err != nil {
		return err
	}
	if worktreeErr == nil {
		if err := os.RemoveAll(want.Worktree); err != nil {
			return err
		}
	}
	return nil
}

func stopAttemptSession(ctx context.Context, manifest agentruntime.Manifest) error {
	if manifest.Version != agentruntime.ManifestVersion2 {
		return errors.New("legacy implementation session has no durable launch identity")
	}
	profileDigest := strings.TrimSpace(os.Getenv("AGENT_SYMPHONY_WORKER_PROFILE_DIGEST"))
	confined := agentruntime.WorkerConfinementBound(manifest, manifest.WorkerGeneration, profileDigest)
	if manifest.LaunchID == "" && confined {
		return nil
	}
	binding, err := agentruntime.ReadImplementationBinding(manifest)
	if err != nil {
		return err
	}
	session := manifest.Session
	probe := func() (bool, error) {
		result, err := runHostTmux(ctx, []string{"has-session", "-t", "=" + session}, nil)
		if err == nil {
			return true, nil
		}
		if exactTmuxSessionAbsent(result, session) {
			return false, nil
		}
		return false, err
	}
	live, err := probe()
	if err != nil {
		return err
	}
	if !live {
		absent, probeErr := hostBoundImplementationPaneAbsent(ctx, binding)
		if probeErr == nil && absent {
			if confined {
				return nil
			}
			gone, groupErr := agentruntime.ImplementationWorkerGone(manifest, binding)
			if groupErr == nil && gone {
				return nil
			}
			return errors.Join(groupErr, errors.New("implementation worker group termination is unconfirmed"))
		}
		return errors.Join(probeErr, errors.New("bound implementation pane may still exist"))
	}
	observed, err := runHostTmux(ctx, []string{"display-message", "-p", "-t", agentruntime.PaneTarget(session), agentruntime.ImplementationPaneFormat}, nil)
	if err != nil {
		return err
	}
	pane, err := agentruntime.ParseImplementationPane(observed.Output)
	if err != nil || !binding.Matches(manifest, pane) {
		return errors.New("implementation pane no longer matches durable launch identity")
	}
	args, err := agentruntime.GuardedImplementationArgs(binding, pane, "kill-pane -t "+pane.PaneID)
	if err != nil {
		return err
	}
	result, err := runHostTmux(ctx, args, nil)
	if err != nil || strings.TrimSpace(result.Output) != "" {
		return errors.Join(err, errors.New("implementation pane changed before guarded cleanup"))
	}
	absent, inventoryErr := hostBoundImplementationPaneAbsent(ctx, binding)
	if inventoryErr != nil {
		return inventoryErr
	}
	if !absent {
		return errors.New("bound implementation pane remained after guarded cleanup")
	}
	if confined {
		return nil
	}
	workerGone, groupErr := agentruntime.ImplementationWorkerGone(manifest, binding)
	if groupErr != nil || !workerGone {
		return errors.Join(groupErr, errors.New("implementation worker group termination is unconfirmed"))
	}
	return nil
}

func hostBoundImplementationPaneAbsent(ctx context.Context, binding agentruntime.ImplementationLaunchBinding) (bool, error) {
	result, err := runHostTmux(ctx, []string{"list-panes", "-a", "-F", agentruntime.ImplementationInventoryFormat}, nil)
	if err != nil {
		if agentruntime.ImplementationOriginalServerGone(binding) {
			return true, nil
		}
		return false, err
	}
	absent, err := agentruntime.ImplementationPaneAbsentFromInventory(result.Output, binding)
	if err != nil && agentruntime.ImplementationOriginalServerGone(binding) {
		return true, nil
	}
	return absent, err
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
	if _, err := boundaryEnvironment(c.Env); err != nil {
		return err
	}
	if !validWorkerControlEnvironment(c.Env, root) {
		return errors.New("invalid worker control environment")
	}
	return nil
}

func validWorkerControlEnvironment(environment []string, root string) bool {
	values := map[string]string{}
	for _, entry := range environment {
		name, value, _ := strings.Cut(entry, "=")
		if !slices.Contains([]string{agentruntime.WorkerStatusEnvironment, agentruntime.WorkerGenerationEnv, agentruntime.WorkerLaunchIDEnv}, name) {
			continue
		}
		if _, duplicate := values[name]; duplicate {
			return false
		}
		values[name] = value
	}
	if len(values) == 0 {
		return true
	}
	generation, err := strconv.ParseUint(values[agentruntime.WorkerGenerationEnv], 10, 64)
	status := values[agentruntime.WorkerStatusEnvironment]
	workspace := filepath.Dir(filepath.Dir(status))
	return len(values) == 3 && generation > 0 && err == nil && agentruntime.ValidLaunchToken(values[agentruntime.WorkerLaunchIDEnv]) &&
		filepath.Dir(workspace) == root && status == agentruntime.StatusPath(workspace)
}

func boundaryEnvironment(environment []string) ([]string, error) {
	requiredGit, _ := internalgithub.WorkerEnvironmentWith(nil)
	managedGit := make(map[string]bool, len(requiredGit))
	for _, entry := range requiredGit {
		managedGit[entry] = false
	}
	names := make([]string, 0, len(environment))
	gitConfigSeen := false
	noSystem := false
	for _, entry := range environment {
		name, _, ok := strings.Cut(entry, "=")
		if !ok || name == "" || strings.ContainsAny(name, " \t\r\n") {
			return nil, errors.New("invalid boundary environment")
		}
		if strings.HasPrefix(strings.ToUpper(name), "GIT_CONFIG") {
			gitConfigSeen = true
			if entry == "GIT_CONFIG_NOSYSTEM=1" && !noSystem {
				noSystem = true
				continue
			}
			seen, managed := managedGit[entry]
			if !managed || seen {
				return nil, errors.New("invalid boundary environment")
			}
			managedGit[entry] = true
			continue
		}
		if reservedHostEnvironment(name) {
			return nil, errors.New("invalid boundary environment")
		}
		names = append(names, name)
	}
	if gitConfigSeen {
		for _, seen := range managedGit {
			if !seen {
				return nil, errors.New("invalid boundary environment")
			}
		}
	}
	filtered, err := internalgithub.WorkerEnvironmentWith(environment, names...)
	if err != nil {
		return nil, err
	}
	if noSystem {
		filtered = append(filtered, "GIT_CONFIG_NOSYSTEM=1")
	}
	return filtered, nil
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
	gate := ""
	if len(args) >= 4 && slices.Equal(args[:2], []string{"wait-for", "-L"}) && args[3] == ";" {
		gate, args = args[2], args[4:]
		if !validImplementationGateChannel(gate) {
			return false
		}
	}
	if len(args) == 0 {
		return false
	}
	if offset := tmuxNewSessionOffset(args); offset >= 0 {
		if offset == 0 || !slices.Equal(strings.Fields(args[3]), environmentNames(environment)) {
			return false
		}
		args = args[offset:]
	}
	if gate != "" && args[0] != "new-session" {
		return false
	}
	switch args[0] {
	case "new-session":
		if len(args) == 6 {
			return args[1] == "-d" && args[2] == "-s" && args[4] == "-c" && boundedCommandPath(args[5], dir, root)
		}
		if gate != "" {
			return validBoundImplementationNewSession(args, gate, dir, root)
		}
		if len(args) >= 17 && args[8] == "review-pane" {
			if args[1] != "-d" || args[2] != "-s" || args[4] != "-c" || !boundedCommandPath(args[5], dir, root) || args[6] != "--" || args[9] != "tmux" || args[15] != "--" || args[16] == "" {
				return false
			}
			helper, err := os.Executable()
			resultRoot := filepath.Dir(args[10])
			resultName := strings.TrimPrefix(filepath.Base(resultRoot), ".agent-symphony-review-")
			resultDigest, digestErr := hex.DecodeString(resultName)
			if err != nil || args[7] != helper || filepath.Base(args[10]) != "launch.json" || filepath.Base(args[11]) != "terminal.json" || resultRoot != filepath.Dir(args[11]) || filepath.Dir(resultRoot) != args[5] || digestErr != nil || len(resultDigest) != 8 || strings.ToLower(resultName) != resultName {
				return false
			}
			var identity reviewerLaunchIdentity
			return json.Unmarshal([]byte(args[14]), &identity) == nil && identity.GateProtocol && identity.SessionRequested && identity.EffectID != "" && identity.IssueGeneration > 0 && identity.AttemptGeneration > 0 && validDigest(identity.RequestDigest) && args[12] == reviewerSignal(identity) && args[13] == reviewerStartSignal(identity)
		}
		return validBoundImplementationNewSession(args, "", dir, root)
	case "has-session", "kill-session":
		return len(args) == 3 && args[1] == "-t" && (validTmuxTarget(args[2], false) || args[0] == "kill-session" && validTmuxSessionID(args[2]))
	case "list-sessions":
		return len(args) == 3 && args[1] == "-F" && args[2] == reviewerSessionsFormat
	case "if-shell":
		return validReviewerGuardedKillArgs(args) || validImplementationGuardedArgs(args)
	case "display-message":
		return len(args) == 5 && args[1] == "-p" && args[2] == "-t" && validTmuxTarget(args[3], true) && slices.Contains([]string{"#{pane_dead}", agentruntime.PaneStatusFormat, reviewerPaneIdentityFormat, agentruntime.ImplementationPaneFormat, "#{pane_start_command}", "#{pane_pid}"}, args[4])
	case "list-panes":
		return len(args) == 4 && slices.Equal(args[1:3], []string{"-a", "-F"}) && args[3] == agentruntime.ImplementationInventoryFormat
	case "wait-for":
		return len(args) == 3 && (args[1] == "-L" || args[1] == "-U") && (validReviewerWaitChannel(args[2]) || validImplementationGateChannel(args[2]))
	case "capture-pane":
		return len(args) == 6 && slices.Equal(args[1:5], []string{"-p", "-S", "-", "-t"}) && validTmuxTarget(args[5], true)
	case "set-option":
		return len(args) == 6 && validTmuxTarget(args[3], true) && (slices.Equal(args[1:3], []string{"-w", "-t"}) && ((args[4] == "remain-on-exit" && args[5] == "on") || (args[4] == "history-limit" && slices.Contains([]string{"5000", "65536"}, args[5]))) || slices.Equal(args[1:3], []string{"-p", "-t"}) && args[5] == "" && slices.Contains([]string{agentruntime.PaneExitStatusOption, agentruntime.PaneExitSignalOption}, args[4]))
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

var reviewerGuardFormatPattern = regexp.MustCompile(`^#\{&&:#\{==:#\{pid\},([1-9][0-9]*)\},#\{&&:#\{==:#\{start_time\},([1-9][0-9]*)\},#\{&&:#\{==:#\{session_name\},(as-r-(?:[0-9a-f]{48}|[0-9a-f]{16}-[1-9][0-9]*-[1-9][0-9]*))\},#\{&&:#\{==:#\{session_id\},(\$[0-9]+)\},#\{==:#\{pane_pid\},([1-9][0-9]*)\}\}\}\}\}$`)

func validReviewerGuardedKillArgs(args []string) bool {
	if len(args) != 7 || args[1] != "-F" || args[2] != "-t" || args[6] != "display-message -p "+reviewerGuardMismatch {
		return false
	}
	fields := reviewerGuardFormatPattern.FindStringSubmatch(args[4])
	if fields == nil || args[3] != "="+fields[3]+":0.0" || args[5] != "kill-session -t "+fields[4] {
		return false
	}
	serverPID, err1 := strconv.Atoi(fields[1])
	startTime, err2 := strconv.ParseUint(fields[2], 10, 64)
	panePID, err3 := strconv.Atoi(fields[5])
	if err1 != nil || err2 != nil || err3 != nil || serverPID < 2 || startTime == 0 || panePID < 2 {
		return false
	}
	return args[4] == reviewerGuardCondition(reviewerPaneIdentity{ServerPID: serverPID, StartTime: startTime, Name: fields[3], SessionID: fields[4], PID: panePID})
}

func validReviewerWaitChannel(channel string) bool {
	if !strings.HasPrefix(channel, "review-") {
		return false
	}
	identity := strings.TrimPrefix(channel, "review-")
	identity = strings.TrimSuffix(identity, "-start")
	identity = strings.TrimSuffix(identity, "-go")
	if len(identity) != 32 {
		return false
	}
	for _, c := range identity {
		if c < '0' || c > '9' && c < 'a' || c > 'f' {
			return false
		}
	}
	return true
}

func validImplementationGateChannel(channel string) bool {
	return strings.HasPrefix(channel, "implementation-") && agentruntime.ValidLaunchToken(strings.TrimPrefix(channel, "implementation-"))
}

func validBoundImplementationNewSession(args []string, gate, dir, root string) bool {
	if len(args) < 45 || !slices.Equal(args[1:4], []string{"-d", "-P", "-F"}) || args[4] != agentruntime.ImplementationPaneFormat || args[5] != "-s" || !validTmuxTarget("="+args[6], false) || args[7] != "-c" || !boundedCommandPath(args[8], dir, root) {
		return false
	}
	target, suffix := agentruntime.PaneTarget(args[6]), args[len(args)-35:]
	token := suffix[6]
	if !agentruntime.ValidLaunchToken(token) || !slices.Equal(suffix, []string{
		";", "set-option", "-p", "-t", target, "@agent-symphony-launch-token", token,
		";", "set-option", "-w", "-t", target, "remain-on-exit", "on",
		";", "set-option", "-w", "-t", target, "history-limit", "5000",
		";", "set-option", "-p", "-t", target, agentruntime.PaneExitStatusOption, "",
		";", "set-option", "-p", "-t", target, agentruntime.PaneExitSignalOption, "",
	}) {
		return false
	}
	launch := args[9 : len(args)-35]
	if gate == "" {
		return slices.Equal(launch, []string{"/bin/sh"})
	}
	helper, err := os.Executable()
	if err != nil || len(launch) < 10 || launch[0] != helper || launch[1] != "implementation-gate" || launch[2] != "tmux" || !validImplementationLogPath(launch[3], args[8], args[6], dir, root) || launch[4] != args[8] || launch[5] != args[6] || launch[6] != token || launch[7] != strings.TrimPrefix(gate, "implementation-") || launch[8] != "--" || launch[9] == "" {
		return false
	}
	return !slices.Contains(launch, ";")
}

func validImplementationLogPath(logPath, worktree, session, dir, root string) bool {
	if boundedCommandPath(logPath, dir, root) {
		return true
	}
	if filepath.Base(root) != "worktrees" || filepath.Dir(worktree) != root || filepath.Base(worktree) != strings.TrimPrefix(session, "as-") {
		return false
	}
	identity := filepath.Base(worktree)
	last := strings.LastIndexByte(identity, '-')
	if last < 1 {
		return false
	}
	previous := strings.LastIndexByte(identity[:last], '-')
	if previous < 1 {
		return false
	}
	repository, issue, attempt := identity[:previous], identity[previous+1:last], identity[last+1:]
	if repository == "" || !positiveDecimal(issue) || !positiveDecimal(attempt) {
		return false
	}
	want := filepath.Join(filepath.Dir(root), "attempts", repository, issue+"-"+attempt, "agent.log")
	return filepath.Clean(logPath) == logPath && logPath == want
}

func positiveDecimal(value string) bool {
	if value == "" || value[0] == '0' {
		return false
	}
	for _, digit := range value {
		if digit < '0' || digit > '9' {
			return false
		}
	}
	return true
}

var implementationGuardPattern = regexp.MustCompile(`^#\{&&:#\{==:#\{pid\},([1-9][0-9]*)\},#\{&&:#\{==:#\{start_time\},([1-9][0-9]*)\},#\{&&:#\{==:#\{session_name\},(as-[a-z0-9][a-z0-9._-]{0,39}-[0-9a-f]{12}-[1-9][0-9]*-[1-9][0-9]*)\},#\{&&:#\{==:#\{session_id\},(\$[0-9]+)\},#\{&&:#\{==:#\{pane_id\},(%[0-9]+)\},#\{&&:#\{==:#\{pane_pid\},([1-9][0-9]*)\},#\{==:#\{@agent-symphony-launch-token\},([0-9a-f]{32})\}\}\}\}\}\}\}$`)

func validImplementationGuardedArgs(args []string) bool {
	if len(args) != 7 || args[1] != "-F" || args[2] != "-t" || args[6] != "display-message -p "+agentruntime.ImplementationGuardMismatch {
		return false
	}
	fields := implementationGuardPattern.FindStringSubmatch(args[4])
	if fields == nil || args[3] != fields[5] {
		return false
	}
	serverPID, err1 := strconv.Atoi(fields[1])
	serverStart, err2 := strconv.ParseUint(fields[2], 10, 64)
	panePID, err3 := strconv.Atoi(fields[6])
	if err1 != nil || err2 != nil || err3 != nil || serverPID < 2 || serverStart == 0 || panePID < 2 {
		return false
	}
	binding := agentruntime.ImplementationLaunchBinding{ServerPID: serverPID, ServerStart: serverStart, SessionName: fields[3], SessionID: fields[4], PaneID: fields[5], PanePID: panePID, Token: fields[7], Command: "validated by host boundary"}
	pane := agentruntime.ImplementationPane{ServerPID: serverPID, ServerStart: serverStart, SessionName: fields[3], SessionID: fields[4], PaneID: fields[5], PanePID: panePID, Token: fields[7], Command: binding.Command}
	condition, err := agentruntime.ImplementationGuardCondition(binding, pane)
	if err != nil || condition != args[4] {
		return false
	}
	if args[5] == "send-keys -t "+pane.PaneID+" C-c" || args[5] == "kill-pane -t "+pane.PaneID || strings.HasPrefix(args[5], "wait-for -U ") && validImplementationGateChannel(strings.TrimPrefix(args[5], "wait-for -U ")) {
		return true
	}
	nested, ok := parseCanonicalTmuxWords(args[5])
	if !ok || len(nested) == 0 {
		return false
	}
	switch nested[0] {
	case "set-option":
		return len(nested) == 6 && slices.Equal(nested[1:4], []string{"-p", "-t", pane.PaneID}) && nested[5] == "" && slices.Contains([]string{agentruntime.PaneExitStatusOption, agentruntime.PaneExitSignalOption}, nested[4])
	case "respawn-pane":
		return len(nested) > 5 && slices.Equal(nested[1:4], []string{"-k", "-t", pane.PaneID}) && nested[4] == "--" && nested[5] != ""
	case "display-message":
		return len(nested) == 5 && slices.Equal(nested[1:4], []string{"-p", "-t", pane.PaneID}) && slices.Contains([]string{agentruntime.PaneStatusFormat, "#{pane_dead}"}, nested[4])
	case "capture-pane":
		return len(nested) == 6 && slices.Equal(nested[1:5], []string{"-p", "-S", "-", "-t"}) && nested[5] == pane.PaneID
	case "load-buffer":
		return len(nested) == 4 && nested[1] == "-b" && nested[2] != "" && nested[3] == "-"
	case "paste-buffer":
		return len(nested) == 6 && slices.Equal(nested[1:3], []string{"-d", "-b"}) && nested[3] != "" && nested[4] == "-t" && nested[5] == pane.PaneID
	case "send-keys":
		return len(nested) == 4 && nested[1] == "-t" && nested[2] == pane.PaneID && nested[3] == "Enter"
	default:
		return false
	}
}

// parseCanonicalTmuxWords accepts only the single-quoted argv encoding emitted
// by runtime.TmuxCommandString, not arbitrary shell syntax.
func parseCanonicalTmuxWords(command string) ([]string, bool) {
	original := command
	var words []string
	for len(command) > 0 {
		if command[0] != '\'' {
			return nil, false
		}
		command = command[1:]
		var word strings.Builder
		for {
			index := strings.IndexByte(command, '\'')
			if index < 0 {
				return nil, false
			}
			word.WriteString(command[:index])
			command = command[index:]
			if strings.HasPrefix(command, "'\\''") {
				word.WriteByte('\'')
				command = command[4:]
				continue
			}
			command = command[1:]
			break
		}
		words = append(words, word.String())
		if command == "" {
			break
		}
		if command[0] != ' ' {
			return nil, false
		}
		command = command[1:]
	}
	canonical, err := agentruntime.TmuxCommandString(words)
	return words, err == nil && canonical == original
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

func validTmuxSessionID(target string) bool {
	if !strings.HasPrefix(target, "$") || len(target) < 2 {
		return false
	}
	_, err := strconv.ParseUint(target[1:], 10, 64)
	return err == nil
}

func reservedHostEnvironment(name string) bool {
	if internalgithub.GitHubCLIEnvironmentVariable(name) {
		return false
	}
	upper := strings.ToUpper(name)
	if strings.HasPrefix(upper, "AGENT_SYMPHONY_") {
		return !slices.Contains([]string{"AGENT_SYMPHONY_IMPLEMENTATION_RESULT", "AGENT_SYMPHONY_REVIEW_RESULT", agentruntime.WorkerStatusEnvironment, agentruntime.WorkerGenerationEnv, agentruntime.WorkerLaunchIDEnv}, upper)
	}
	if upper == "HOME" || upper == "TMUX_TMPDIR" {
		return true
	}
	for _, part := range []string{"TOKEN", "SECRET", "PASSWORD", "PASSWD", "PRIVATE_KEY", "PRIVATE-KEY", "CREDENTIAL", "AUTHORIZATION", "GITHUB_PAT"} {
		if strings.Contains(upper, part) {
			return true
		}
	}
	for _, prefix := range []string{"GITHUB_", "GH_", "SSH_", "AWS_", "AZURE_", "GOOGLE_", "GCP_", "CLOUD_", "OCI_", "CLOUDFLARE_", "DIGITALOCEAN_", "GIT_ASKPASS", "GIT_CONFIG", "APP_"} {
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
	if identityErr != nil || !agentruntime.ValidManifestVersion(manifest) || manifest.State != "completed" || manifest.Branch != want.Branch || manifest.Worktree != want.Worktree || manifest.Session != want.Session {
		return "", errors.New("invalid export manifest")
	}
	run := func(args ...string) (string, error) {
		cmd := exec.CommandContext(ctx, "git", append([]string{"--no-optional-locks", "-c", "core.hooksPath=/dev/null", "-C", manifest.Worktree}, args...)...)
		cmd.Env = append(minimalBoundaryEnvironment(), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_TERMINAL_PROMPT=0")
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		out, err := cmd.Output()
		if err != nil {
			return strings.TrimSpace(string(out)), fmt.Errorf("git command failed: %w: %s", err, strings.TrimSpace(stderr.String()))
		}
		return strings.TrimSpace(string(out)), nil
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
	status, err := run("status", "--porcelain", "--", ".", ":(exclude).agent-symphony", ":(exclude).agents", ":(exclude).codex")
	if err != nil {
		return "", errors.New("inspect export worktree")
	}
	if status != "" {
		if _, err := run("add", "--all", "--", ".", ":(exclude).agent-symphony", ":(exclude).agents", ":(exclude).codex"); err != nil {
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
	if status, err := run("status", "--porcelain", "--", ".", ":(exclude).agent-symphony", ":(exclude).agents", ":(exclude).codex"); err != nil || status != "" {
		return "", errors.New("export worktree is not clean")
	}
	tmp, err := os.CreateTemp(manifest.Worktree, ".agent-symphony-export-*.bundle")
	if err != nil {
		return "", err
	}
	name := tmp.Name()
	if err := tmp.Close(); err != nil {
		return "", err
	}
	if err := os.Remove(name); err != nil {
		return "", err
	}
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
	if request.Manifest.Version == agentruntime.ManifestVersion2 {
		decodedID, idErr := hex.DecodeString(request.CandidateLaunchID)
		if !agentruntime.ValidManifestVersion(request.Manifest) || !agentruntime.ValidLaunchToken(request.CandidateLaunchToken) || request.CandidateLaunchToken == request.Manifest.LaunchToken || idErr != nil || len(decodedID) != 16 {
			return request, h, errors.New("invalid handoff candidate identity")
		}
	} else if request.CandidateLaunchToken != "" || request.CandidateLaunchID != "" {
		return request, h, errors.New("unexpected handoff candidate identity")
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
		CandidateLaunchToken       string `json:",omitempty"`
		CandidateLaunchID          string `json:",omitempty"`
	}{"pending", request.Manifest.Worktree, request.Manifest.Session, request.Manifest.LogPath, request.Handoff, request.OutcomePath, request.OutcomeToken, request.Command, request.CandidateLaunchToken, request.CandidateLaunchID})
	return binding, fmt.Sprintf("%x", sha256.Sum256(binding))
}

func verifyHandoff(ctx context.Context, input []byte, root string) (string, error) {
	request, h, err := decodeHandoffRequest(input, root)
	if err != nil {
		return "", err
	}
	handoffProof, recipient := handoffBinding(request)
	persisted, err := immutableMarkerMatches(filepath.Join(request.Manifest.Worktree, ".agent-symphony", "handoffs", h.Key+".json"), handoffProof)
	if err != nil {
		return "", fmt.Errorf("verify handoff binding: %w", err)
	}
	if !persisted {
		return "", nil
	}
	if request.Manifest.Version != agentruntime.ManifestVersion2 {
		return "", errors.New("legacy implementation pane has no durable handoff identity")
	}
	option := "@agent-symphony-handoff-" + recipient[:16]
	binding, pane, err := handoffCandidateBinding(ctx, request)
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	read, _ := agentruntime.TmuxCommandString([]string{"show-options", "-pqv", "-t", pane.PaneID, option})
	args, err := agentruntime.GuardedImplementationArgs(binding, pane, read)
	if err != nil {
		return "", err
	}
	observed, err := runHostTmux(ctx, args, nil)
	if err != nil || strings.TrimSpace(observed.Output) != recipient {
		return "", err
	}
	ack, _ := json.Marshal(handoffReceipt{"agent-symphony-handoff-executed-v1", h.Key, request.OutcomePath, request.OutcomeToken})
	matches, err := immutableMarkerMatches(request.OutcomePath, ack)
	if err != nil || !matches {
		return "", err
	}
	return string(ack), nil
}

func acceptHandoff(_ context.Context, input []byte, root string) (string, error) {
	request, _, err := decodeHandoffRequest(input, root)
	if err != nil {
		return "", err
	}
	if request.Manifest.Version == agentruntime.ManifestVersion2 {
		return "", errors.New("bound handoff requires prepare and release phases")
	}
	return "", errors.New("legacy implementation pane has no durable handoff identity")
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

func hostDiagnostic(codex, stateRoot string) diagnostic {
	if strings.TrimSpace(stateRoot) == "" {
		return diagnostic{"worker confinement", "fail", "runtime state root is required to provision the local attempt/snapshot roots", "Pass --runtime-state."}
	}
	if err := validatePrivateStateRoot(stateRoot); err != nil {
		return diagnostic{"worker confinement", "fail", err.Error(), "Choose a private persistent --runtime-state path under the current user's home."}
	}
	for _, root := range []string{localAttemptRoot(stateRoot), localSnapshotRoot(stateRoot)} {
		if err := verifyLocalAccess(root); err != nil {
			return diagnostic{"worker confinement", "fail", err.Error(), "Repair " + root + " ownership and mode."}
		}
	}
	if err := configureProjectRuntimeState(stateRoot); err != nil {
		return diagnostic{"worker confinement", "fail", err.Error(), "Repair the runtime state root and coordinator Codex installation."}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	proof, err := rootlessCodexVerify(ctx, localAttemptRoot(stateRoot), workerCodexHome(stateRoot), codex)
	if err != nil {
		return diagnostic{"worker confinement", "fail", err.Error(), "Install @openai/codex@0.153.0 and repair the managed sandbox profile. On Linux/WSL, the host must permit unprivileged user namespaces for bubblewrap."}
	}
	message := "real managed Codex sandbox confinement proof passed"
	if hostIsolationInstalled() {
		message += "; legacy install-host identities are present but unused"
	}
	if proof.SharedTempRead || proof.SharedTempWrite {
		return diagnostic{"worker confinement", "warn", message + "; Codex cannot isolate shared temporary files on this platform", "Keep Agent Symphony authority and control paths under the private runtime state root."}
	}
	return diagnostic{"worker confinement", "pass", message, ""}
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
