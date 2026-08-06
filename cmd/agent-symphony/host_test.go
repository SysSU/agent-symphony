package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

func fakeHostIdentity(t *testing.T, uid, gid int) {
	t.Helper()
	oldEUID, oldEGID, oldUser, oldGroup, oldOutput := hostEUID, hostEGID, hostLookupUser, hostLookupGroup, hostOutput
	hostEUID, hostEGID = func() int { return uid }, func() int { return gid }
	hostLookupUser = func(name string) (*user.User, error) {
		return &user.User{Username: name, Uid: "1234", Gid: "5678", HomeDir: "/var/lib/" + name}, nil
	}
	hostLookupGroup = func(name string) (*user.Group, error) { return &user.Group{Name: name, Gid: "5678"}, nil }
	hostOutput = func(name string, args ...string) ([]byte, error) {
		if name == "getent" {
			if args[0] == "group" {
				return []byte(args[len(args)-1] + ":x:5678:\n"), nil
			}
			account := args[len(args)-1]
			return []byte(account + ":x:1234:5678::/var/lib/" + account + ":/usr/sbin/nologin\n"), nil
		}
		return []byte("5678\n"), nil
	}
	t.Cleanup(func() {
		hostEUID, hostEGID, hostLookupUser, hostLookupGroup, hostOutput = oldEUID, oldEGID, oldUser, oldGroup, oldOutput
	})
}

func TestAgentHostRunsBoundedCommandWithFilteredEnvironment(t *testing.T) {
	fakeHostIdentity(t, 1234, 5678)
	oldGOOS, oldRoot, oldExec := hostGOOS, hostRoot, hostExecRunner
	hostGOOS = "linux"
	hostRoot = t.TempDir()
	t.Cleanup(func() { hostGOOS, hostRoot, hostExecRunner = oldGOOS, oldRoot, oldExec })
	hostExecRunner = func(_ context.Context, command agentruntime.Command) (agentruntime.Result, error) {
		return agentruntime.Result{Output: strings.Join(command.Env, "|")}, nil
	}
	for mode, spec := range map[string]struct{ root, home string }{
		"implementation": {"/var/lib/agent-symphony/attempts", "/var/lib/agent-symphony-worker"},
		"review":         {"/var/lib/agent-symphony/snapshots", "/var/lib/agent-symphony-reviewer"},
	} {
		t.Run(mode, func(t *testing.T) {
			dir := filepath.Join(nativeRoot(spec.root), "fake-test")
			if err := os.MkdirAll(dir, 0o700); err != nil {
				t.Skipf("cannot create fake boundary root: %v", err)
			}
			payload, _ := json.Marshal(struct {
				Operation string          `json:"operation"`
				Command   boundaryCommand `json:"command"`
			}{"run", boundaryCommand{Name: "git", Args: []string{"-C", dir, "rev-parse", "HEAD"}, Dir: dir, Env: []string{"MODEL_API_KEY=model-canary", "PATH=/bin"}}})
			var out bytes.Buffer
			if err := agentHost(t.Context(), mode, bytes.NewReader(payload), &out); err != nil {
				t.Fatal(err)
			}
			var result agentruntime.Result
			if err := json.Unmarshal(out.Bytes(), &result); err != nil || !strings.Contains(result.Output, "MODEL_API_KEY=model-canary") || !strings.Contains(result.Output, "HOME="+spec.home) || strings.Contains(result.Output, os.Getenv("HOME")) || strings.Contains(result.Output, "GITHUB_TOKEN") {
				t.Fatalf("result=%#v err=%v", result, err)
			}
		})
	}
}

func TestHandoffPersistenceAndExportStayBounded(t *testing.T) {
	if _, err := exec.LookPath("tmux"); err != nil {
		t.Skip("tmux is unavailable")
	}
	tmuxTmp, err := os.MkdirTemp("/tmp", "as-tmux-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(tmuxTmp) })
	t.Setenv("TMUX_TMPDIR", tmuxTmp)

	t.Run("findings persist without a separate signal", func(t *testing.T) {
		oldExec := hostExecRunner
		calls := 0
		recipient := ""
		hostExecRunner = func(_ context.Context, command agentruntime.Command) (agentruntime.Result, error) {
			calls++
			if slices.Contains(command.Args, "show-options") {
				return agentruntime.Result{Output: recipient}, nil
			}
			if slices.Contains(command.Args, "set-option") {
				recipient = command.Args[len(command.Args)-1]
			}
			return agentruntime.Result{}, nil
		}
		t.Cleanup(func() { hostExecRunner = oldExec })
		worktree := t.TempDir()
		handoff := []byte(`{"type":"agent-symphony-handoff-v1","key":"pane-test"}`)
		request, _ := json.Marshal(struct {
			Manifest     agentruntime.Manifest `json:"manifest"`
			Handoff      json.RawMessage       `json:"handoff"`
			OutcomePath  string                `json:"outcome_path"`
			OutcomeToken string                `json:"outcome_token"`
			Command      []string              `json:"command"`
		}{agentruntime.Manifest{Worktree: worktree, Session: "as-23-1", LogPath: filepath.Join(worktree, "attempt.log")}, handoff, filepath.Join(worktree, "attempt.log.review-outcome"), "token", []string{"implementation"}})
		if _, err := acceptHandoff(t.Context(), request, worktree); err != nil {
			t.Fatal(err)
		}
		if _, err := acceptHandoff(t.Context(), request, worktree); err != nil {
			t.Fatal(err)
		}
		if calls != 3 {
			t.Fatalf("worker made %d tmux calls, want one lookup plus one two-step delivery", calls)
		}
		if _, err := os.ReadFile(filepath.Join(worktree, ".agent-symphony", "handoffs", "pane-test.json")); err != nil {
			t.Fatalf("durable handoff: %v", err)
		}
	})

	t.Run("export", func(t *testing.T) {
		root := t.TempDir()
		primary := filepath.Join(root, "primary")
		if err := os.Mkdir(primary, 0o700); err != nil {
			t.Fatal(err)
		}
		for _, args := range [][]string{{"init"}, {"config", "user.email", "test@example.invalid"}, {"config", "user.name", "test"}} {
			if out, err := exec.Command("git", append([]string{"-C", primary}, args...)...).CombinedOutput(); err != nil {
				t.Fatalf("git %v: %v: %s", args, err, out)
			}
		}
		if err := os.WriteFile(filepath.Join(primary, "README.md"), []byte("base\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		for _, args := range [][]string{{"add", "README.md"}, {"commit", "-m", "base"}} {
			if out, err := exec.Command("git", append([]string{"-C", primary}, args...)...).CombinedOutput(); err != nil {
				t.Fatalf("git %v: %v: %s", args, err, out)
			}
		}
		base := strings.TrimSpace(string(mustOutput(t, exec.Command("git", "-C", primary, "rev-parse", "HEAD"))))
		identity, err := agentruntime.AttemptIdentity(root, agentruntime.Attempt{Repository: "o/r", Issue: 23, Number: 1, BaseSHA: base})
		if err != nil {
			t.Fatal(err)
		}
		if out, err := exec.Command("git", "-C", primary, "worktree", "add", "-b", identity.Branch, identity.Worktree).CombinedOutput(); err != nil {
			t.Fatalf("git worktree: %v: %s", err, out)
		}
		if err := os.WriteFile(filepath.Join(identity.Worktree, "README.md"), []byte("base\nchanged\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		validation := "pane-zero-" + strings.Repeat("v", 240)
		good := fmt.Sprintf(`{"type":"agent-symphony-result-v1","validation":%q,"documentation":"done"}`, validation)
		bad := `{"type":"agent-symphony-result-v1","validation":"wrong-window","documentation":"wrong"}`
		if err := os.WriteFile(filepath.Join(identity.Worktree, workerResultPath), []byte(good), 0o600); err != nil {
			t.Fatal(err)
		}
		tmux(t, "new-session", "-d", "-x", "80", "-s", identity.Session, "-c", identity.Worktree, "sh", "-c", "printf '%s\\n%s\\n' '"+good+"' '"+good+"'; sleep 30")
		t.Cleanup(func() { _ = exec.Command("tmux", "kill-session", "-t", "="+identity.Session).Run() })
		for deadline := time.Now().Add(2 * time.Second); ; time.Sleep(10 * time.Millisecond) {
			out, _ := exec.Command("tmux", "capture-pane", "-p", "-J", "-t", agentruntime.PaneTarget(identity.Session), "-S", "-200").CombinedOutput()
			if strings.Contains(string(out), good) {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("pane zero did not emit result: %s", out)
			}
		}
		tmux(t, "new-window", "-d", "-t", "="+identity.Session, "sh", "-c", "printf '%s\\n' '"+bad+"'; sleep 30")
		tmux(t, "select-window", "-t", "="+identity.Session+":1")
		manifest := identity
		manifest.State = "completed"
		t.Setenv("AGENT_SYMPHONY_LOCAL_ROOT", root)
		call := func(mode string, candidate agentruntime.Manifest) (agentruntime.Result, error) {
			body, _ := json.Marshal(candidate)
			request, _ := json.Marshal(struct {
				Operation string          `json:"operation"`
				Command   boundaryCommand `json:"command"`
			}{"export", boundaryCommand{Input: body}})
			var boundary bytes.Buffer
			err := agentHost(t.Context(), mode, bytes.NewReader(request), &boundary)
			var result agentruntime.Result
			if err == nil {
				err = json.Unmarshal(boundary.Bytes(), &result)
			}
			return result, err
		}
		for name, edit := range map[string]func(*agentruntime.Manifest){
			"sibling worktree": func(m *agentruntime.Manifest) { m.Worktree = primary },
			"wrong session":    func(m *agentruntime.Manifest) { m.Session = "as-sibling" },
			"invalid base":     func(m *agentruntime.Manifest) { m.BaseSHA = "not-a-commit" },
		} {
			t.Run(name, func(t *testing.T) {
				candidate := manifest
				edit(&candidate)
				if _, err := call("implementation", candidate); err == nil {
					t.Fatal("forged manifest was accepted")
				}
			})
		}
		result, err := call("implementation", manifest)
		if err != nil {
			t.Fatal(err)
		}
		var exported workerExport
		if err := json.Unmarshal([]byte(result.Output), &exported); err != nil || exported.Result.Validation != validation || exported.HeadSHA == base {
			t.Fatalf("export=%#v err=%v", exported, err)
		}
		if _, err := os.Lstat(filepath.Join(identity.Worktree, workerResultPath)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("result artifact remained after export: %v", err)
		}
		if _, err := call("review", manifest); err == nil {
			t.Fatal("review boundary accepted implementation export")
		}
		if status := strings.TrimSpace(string(mustOutput(t, exec.Command("git", "-C", identity.Worktree, "status", "--porcelain")))); status != "" {
			t.Fatalf("committed worktree remains dirty: %q", status)
		}
		if got := strings.TrimSpace(string(mustOutput(t, exec.Command("git", "-C", identity.Worktree, "show", "HEAD:README.md")))); got != "base\nchanged" {
			t.Fatalf("committed content = %q", got)
		}
		if got := strings.TrimSpace(string(mustOutput(t, exec.Command("git", "-C", primary, "rev-parse", "HEAD")))); got != base {
			t.Fatalf("primary worktree moved from %s to %s", base, got)
		}
	})
}

func TestWorkerResultArtifactFailsClosed(t *testing.T) {
	valid := `{"type":"agent-symphony-result-v1","validation":"go test ./... passed","documentation":"none"}`
	malformed := `{"type":"agent-symphony-result-v1","validation":`
	unknown := `{"type":"agent-symphony-result-v1","validation":"ok","documentation":"none","extra":true}`
	oversized := `{"type":"agent-symphony-result-v1","validation":"` + strings.Repeat("x", 64<<10) + `","documentation":"none"}`
	for _, test := range []struct {
		name, body string
	}{
		{"missing", ""},
		{"malformed", malformed},
		{"malformed then valid", malformed + "\n" + valid},
		{"unknown field", unknown},
		{"empty validation", `{"type":"agent-symphony-result-v1","validation":" ","documentation":"none"}`},
		{"empty documentation", `{"type":"agent-symphony-result-v1","validation":"ok","documentation":" "}`},
		{"multiple objects", valid + "\n" + valid},
		{"oversized", oversized},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			if test.body != "" {
				if err := os.WriteFile(filepath.Join(dir, workerResultPath), []byte(test.body), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if result, err := readWorkerResult(dir); err == nil {
				t.Fatalf("result=%#v", result)
			}
		})
	}
	t.Run("symlink", func(t *testing.T) {
		dir := t.TempDir()
		target := filepath.Join(t.TempDir(), "result")
		if err := os.WriteFile(target, []byte(valid), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(target, filepath.Join(dir, workerResultPath)); err != nil {
			t.Fatal(err)
		}
		if _, err := readWorkerResult(dir); err == nil {
			t.Fatal("symlink result was accepted")
		}
	})
}

func TestPendingHandoffRetriesWithoutDuplicateExecution(t *testing.T) {
	for _, failure := range []string{"load-buffer", "submission", "receipt"} {
		t.Run(failure, func(t *testing.T) {
			oldExec, oldDirSync := hostExecRunner, immutableDirSync
			t.Cleanup(func() { hostExecRunner, immutableDirSync = oldExec, oldDirSync })
			worktree := t.TempDir()
			handoff := []byte(`{"type":"agent-symphony-handoff-v1","key":"retry-key"}`)
			request, _ := json.Marshal(struct {
				Manifest     agentruntime.Manifest `json:"manifest"`
				Handoff      json.RawMessage       `json:"handoff"`
				OutcomePath  string                `json:"outcome_path"`
				OutcomeToken string                `json:"outcome_token"`
				Command      []string              `json:"command"`
			}{agentruntime.Manifest{Worktree: worktree, Session: "as-retry", LogPath: filepath.Join(worktree, "attempt.log")}, handoff, filepath.Join(worktree, "attempt.log.review-outcome"), "token", []string{"implementation"}})
			recipient, deliveries, injected := "", 0, false
			hostExecRunner = func(_ context.Context, command agentruntime.Command) (agentruntime.Result, error) {
				if slices.Contains(command.Args, "show-options") {
					return agentruntime.Result{Output: recipient}, nil
				}
				if !injected && failure == "load-buffer" && slices.Contains(command.Args, "load-buffer") {
					injected = true
					return agentruntime.Result{}, errors.New("injected load failure")
				}
				if slices.Contains(command.Args, "set-option") {
					if !injected && failure == "submission" {
						injected = true
						return agentruntime.Result{}, errors.New("injected submission failure")
					}
					recipient, deliveries = command.Args[len(command.Args)-1], deliveries+1
				}
				return agentruntime.Result{}, nil
			}
			immutableDirSync = func(dir string) error {
				if !injected && failure == "receipt" && dir == worktree && recipient != "" {
					injected = true
					return errors.New("injected receipt sync failure")
				}
				return oldDirSync(dir)
			}
			if _, err := acceptHandoff(t.Context(), request, worktree); err == nil {
				t.Fatal("injected failure succeeded")
			}
			if _, err := os.Stat(filepath.Join(worktree, ".agent-symphony", "handoffs", "retry-key.json")); err != nil {
				t.Fatalf("pending state lost: %v", err)
			}
			if _, err := acceptHandoff(t.Context(), request, worktree); err != nil {
				t.Fatalf("restart retry: %v", err)
			}
			if deliveries != 1 {
				t.Fatalf("deliveries=%d, want 1", deliveries)
			}
		})
	}
}

func tmux(t *testing.T, args ...string) {
	t.Helper()
	if out, err := exec.Command("tmux", args...).CombinedOutput(); err != nil {
		t.Fatalf("tmux %v: %v: %s", args, err, out)
	}
}

func TestProductionSeedClonesThroughAgentHostBoundary(t *testing.T) {
	fakeHostIdentity(t, 1234, 5678)
	oldGOOS, oldRoot, oldExec := hostGOOS, hostRoot, hostExecRunner
	hostGOOS, hostRoot, hostExecRunner = "linux", t.TempDir(), (agentruntime.ExecRunner{}).Run
	t.Cleanup(func() { hostGOOS, hostRoot, hostExecRunner = oldGOOS, oldRoot, oldExec })
	source := t.TempDir()
	for _, args := range [][]string{{"init"}, {"config", "user.email", "test@example.invalid"}, {"config", "user.name", "test"}, {"commit", "--allow-empty", "-m", "base"}} {
		if out, err := exec.Command("git", append([]string{"-C", source}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	root := nativeRoot("/var/lib/agent-symphony/attempts")
	bundle, err := seedAttemptSource(t.Context(), source, root)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(bundle)
	if err != nil || info.Mode().Perm() != 0o640 {
		t.Fatalf("seed info=%v err=%v", info, err)
	}
	destination := filepath.Join(root, "production-attempt")
	payload, _ := json.Marshal(struct {
		Operation string          `json:"operation"`
		Command   boundaryCommand `json:"command"`
	}{"run", boundaryCommand{Name: "git", Args: []string{"clone", "--no-local", "--no-checkout", bundle, destination}, Dir: root, Env: DefaultEnvironmentForTest()}})
	var out bytes.Buffer
	if err := agentHost(t.Context(), "implementation", bytes.NewReader(payload), &out); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(destination, ".git")); err != nil {
		t.Fatal(err)
	}
}

func TestLocalSeedAndDiscoveryUseSeparateRoots(t *testing.T) {
	fakeNoHostIsolation(t)
	source := t.TempDir()
	for _, args := range [][]string{{"init"}, {"config", "user.email", "test@example.invalid"}, {"config", "user.name", "test"}, {"commit", "--allow-empty", "-m", "base"}} {
		if out, err := exec.Command("git", append([]string{"-C", source}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	stateRoot := t.TempDir()
	worktreeRoot := productionAttemptRoot(stateRoot)
	bundle, err := seedAttemptSource(t.Context(), source, worktreeRoot)
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Dir(bundle) != filepath.Join(stateRoot, "worktrees") {
		t.Fatalf("bundle shares recovery root: %s", bundle)
	}
	attempts := filepath.Join(stateRoot, "attempts")
	if err := os.Mkdir(attempts, 0o700); err != nil {
		t.Fatal(err)
	}
	legacy := filepath.Join(attempts, "source.bundle")
	if err := os.WriteFile(legacy, []byte("legacy bundle"), 0o640); err != nil {
		t.Fatal(err)
	}
	runtime := agentruntime.Runtime{Root: worktreeRoot, StateRoot: stateRoot}
	if manifests, err := runtime.Discover(); err != nil || len(manifests) != 0 {
		t.Fatalf("discover seeded layout = %#v, %v", manifests, err)
	}
	if err := os.WriteFile(filepath.Join(attempts, "unexpected"), []byte("bad"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Discover(); err == nil || !strings.Contains(err.Error(), "non-symlink directory") {
		t.Fatalf("unexpected state file accepted: %v", err)
	}
	if err := os.Remove(filepath.Join(attempts, "unexpected")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(legacy); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(bundle, legacy); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Discover(); err == nil || !strings.Contains(err.Error(), "non-symlink directory") {
		t.Fatalf("legacy bundle symlink accepted: %v", err)
	}
}

func DefaultEnvironmentForTest() []string { return []string{"PATH=" + os.Getenv("PATH"), "LANG=C"} }

func TestAgentHostRejectsWrongIdentityCredentialAndEscapingCommand(t *testing.T) {
	fakeHostIdentity(t, 1, 2)
	if err := agentHost(t.Context(), "implementation", strings.NewReader(`{"operation":"verify","command":{}}`), &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "must run as") {
		t.Fatalf("err=%v", err)
	}
	fakeHostIdentity(t, 1234, 5678)
	for _, command := range []boundaryCommand{
		{Name: "sh"},
		{Name: filepath.Join(t.TempDir(), "git")},
		{Name: "git", Dir: "/tmp"},
		{Name: "git", Args: []string{"-C", "/tmp"}},
		{Name: "git", Args: []string{"-C", "../../outside", "rev-parse", "HEAD"}, Dir: filepath.Join(nativeRoot("/var/lib/agent-symphony/attempts"), "inside")},
		{Name: "git", Args: []string{"clone", "--no-local", "--no-checkout", "../../outside", "clone"}, Dir: filepath.Join(nativeRoot("/var/lib/agent-symphony/attempts"), "inside")},
		{Name: "tmux", Args: []string{"-S", "../../outside/socket", "has-session", "-t", "=x"}, Dir: filepath.Join(nativeRoot("/var/lib/agent-symphony/attempts"), "inside")},
		{Name: "tmux", Args: []string{"-f", "../../outside.conf", "new-session", "-d", "-s", "x", "-c", "."}, Dir: filepath.Join(nativeRoot("/var/lib/agent-symphony/attempts"), "inside")},
		{Name: "git", Env: []string{"GITHUB_TOKEN=secret-canary"}},
		{Name: "git", Env: []string{"HTTPS_PROXY=https://secret-canary@example.invalid"}},
		{Name: "git", Env: []string{"HOME=/coordinator-home"}},
	} {
		payload, _ := json.Marshal(struct {
			Operation string          `json:"operation"`
			Command   boundaryCommand `json:"command"`
		}{"run", command})
		if err := agentHost(t.Context(), "implementation", bytes.NewReader(payload), &bytes.Buffer{}); err == nil {
			t.Fatalf("accepted %#v", command)
		}
	}
}

func TestBoundaryRejectsSymlinkEscapeForMissingTarget(t *testing.T) {
	root, outside := t.TempDir(), t.TempDir()
	if err := os.Symlink(outside, filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	if belowRoot(filepath.Join(root, "link", "missing"), root) {
		t.Fatal("accepted path through symlinked ancestor")
	}
}

func TestAgentHostRejectsEveryCLIFlag(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"agent-host", "implementation", "--offline"}, &stdout, &stderr); code != 2 || !strings.Contains(stderr.String(), "usage:") {
		t.Fatalf("code=%d stderr=%q", code, stderr.String())
	}
}

func TestProvisionIdentitiesReusesNativeCommandsAndRejectsConflict(t *testing.T) {
	oldGOOS, oldRun, oldUser, oldGroup, oldOutput := hostGOOS, hostRun, hostLookupUser, hostLookupGroup, hostOutput
	t.Cleanup(func() {
		hostGOOS, hostRun, hostLookupUser, hostLookupGroup, hostOutput = oldGOOS, oldRun, oldUser, oldGroup, oldOutput
	})
	hostGOOS = "linux"
	hostLookupUser = func(name string) (*user.User, error) {
		return &user.User{Username: name, Uid: "100", Gid: "200", HomeDir: "/var/lib/" + name}, nil
	}
	hostLookupGroup = func(name string) (*user.Group, error) { return &user.Group{Name: name, Gid: "200"}, nil }
	hostOutput = func(name string, args ...string) ([]byte, error) {
		if name == "getent" {
			if args[0] == "group" {
				return []byte(args[len(args)-1] + ":x:200:\n"), nil
			}
			account := args[len(args)-1]
			return []byte(account + ":x:100:200::/var/lib/" + account + ":/usr/sbin/nologin\n"), nil
		}
		return []byte("200\n"), nil
	}
	var calls []string
	hostRun = func(name string, args ...string) error {
		calls = append(calls, strings.Join(append([]string{name}, args...), " "))
		return nil
	}
	if err := provisionIdentities("coordinator"); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 1 || calls[0] != "usermod --append --groups agent-symphony-attempt,agent-symphony-snapshot coordinator" {
		t.Fatalf("calls=%v", calls)
	}
	hostLookupGroup = func(name string) (*user.Group, error) { return &user.Group{Name: name, Gid: "999"}, nil }
	if err := provisionIdentities("coordinator"); err == nil || !strings.Contains(err.Error(), "conflicting") {
		t.Fatalf("err=%v", err)
	}
}

func TestProvisionedIdentitiesRejectUIDAndGIDCollisions(t *testing.T) {
	oldUser, oldGroup := hostLookupUser, hostLookupGroup
	t.Cleanup(func() { hostLookupUser, hostLookupGroup = oldUser, oldGroup })

	for _, test := range []struct {
		name     string
		userIDs  map[string]string
		groupIDs map[string]string
	}{
		{"worker reviewer UID", map[string]string{"coordinator": "1000", workerUser: "1001", reviewerUser: "1001"}, map[string]string{attemptGroup: "2001", snapshotGroup: "2002"}},
		{"attempt snapshot GID", map[string]string{"coordinator": "1000", workerUser: "1001", reviewerUser: "1002"}, map[string]string{attemptGroup: "2001", snapshotGroup: "2001"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			hostLookupUser = func(name string) (*user.User, error) { return &user.User{Username: name, Uid: test.userIDs[name]}, nil }
			hostLookupGroup = func(name string) (*user.Group, error) { return &user.Group{Name: name, Gid: test.groupIDs[name]}, nil }
			if err := validateProvisionedIdentitySeparation("coordinator"); err == nil || !strings.Contains(err.Error(), "share") {
				t.Fatalf("err=%v", err)
			}
		})
	}
}

func TestHostRootCreationRetriesAfterEveryMutationBoundary(t *testing.T) {
	for _, failAt := range []int{1, 2, 3} {
		t.Run(strconv.Itoa(failAt), func(t *testing.T) {
			oldRun, oldUser, oldGroup := hostRun, hostLookupUser, hostLookupGroup
			t.Cleanup(func() { hostRun, hostLookupUser, hostLookupGroup = oldRun, oldUser, oldGroup })
			uid, gid := strconv.Itoa(os.Getuid()), strconv.Itoa(os.Getgid())
			hostLookupUser = func(string) (*user.User, error) { return &user.User{Uid: uid}, nil }
			hostLookupGroup = func(string) (*user.Group, error) { return &user.Group{Gid: gid}, nil }
			calls := 0
			hostRun = func(name string, args ...string) error {
				calls++
				if calls == failAt {
					return os.ErrPermission
				}
				if name == "chmod" {
					mode, _ := strconv.ParseUint(args[0], 8, 32)
					return os.Chmod(args[1], os.FileMode(mode))
				}
				return nil
			}
			path := filepath.Join(t.TempDir(), "attempts")
			if err := ensureHostRoot(path, "worker", "attempt", "2770"); err == nil {
				t.Fatal("injected failure passed")
			}
			hostRun = func(name string, args ...string) error {
				if name == "chmod" {
					mode, _ := strconv.ParseUint(args[0], 8, 32)
					return os.Chmod(args[1], os.FileMode(mode))
				}
				return nil
			}
			if err := ensureHostRoot(path, "worker", "attempt", "2770"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestDarwinIdentityRecordRetriesAfterEveryDSCLMutation(t *testing.T) {
	properties := [][2]string{{"UniqueID", "401"}, {"PrimaryGroupID", "402"}, {"NFSHomeDirectory", "/var/db/test"}, {"UserShell", "/usr/bin/false"}, {"IsHidden", "1"}}
	for failAt := 1; failAt <= len(properties)+2; failAt++ {
		t.Run(strconv.Itoa(failAt), func(t *testing.T) {
			oldRun := hostRun
			t.Cleanup(func() { hostRun = oldRun })
			calls := 0
			hostRun = func(string, ...string) error {
				calls++
				if calls == failAt {
					return os.ErrPermission
				}
				return nil
			}
			if err := ensureDarwinRecord("/Users/test", properties); err == nil {
				t.Fatal("injected failure passed")
			}
			hostRun = func(string, ...string) error { return nil }
			if err := ensureDarwinRecord("/Users/test", properties); err != nil {
				t.Fatal(err)
			}
		})
	}
	for failAt := 1; failAt <= 4; failAt++ {
		t.Run("group-"+strconv.Itoa(failAt), func(t *testing.T) {
			oldRun := hostRun
			t.Cleanup(func() { hostRun = oldRun })
			calls := 0
			hostRun = func(string, ...string) error {
				calls++
				if calls == failAt {
					return os.ErrPermission
				}
				return nil
			}
			if err := ensureDarwinRecord("/Groups/test", [][2]string{{"PrimaryGroupID", "402"}, {"Password", "*"}}); err == nil {
				t.Fatal("injected failure passed")
			}
			hostRun = func(string, ...string) error { return nil }
			if err := ensureDarwinRecord("/Groups/test", [][2]string{{"PrimaryGroupID", "402"}, {"Password", "*"}}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestBroadSudoAuthorityRejectsAnythingButManagedTuples(t *testing.T) {
	binary := "/usr/local/libexec/agent-symphony/1/agent-symphony"
	managed := []byte("    (agent-symphony-worker : agent-symphony-attempt) NOPASSWD: " + binary + " agent-host implementation\n    (agent-symphony-reviewer : agent-symphony-snapshot) NOPASSWD: " + binary + " agent-host review\n")
	if !exactSudoAuthority(managed, binary) {
		t.Fatal("managed tuple rejected")
	}
	for _, rule := range []string{
		strings.Replace(string(managed), binary, "/old/agent-symphony", 1),
		strings.Replace(string(managed), "agent-symphony-worker : agent-symphony-attempt", "root : agent-symphony-attempt", 1),
		string(managed) + "(root) NOPASSWD: /usr/bin/id\n",
	} {
		if exactSudoAuthority([]byte(rule), binary) {
			t.Fatalf("broad rule accepted: %s", rule)
		}
	}
}

func TestBinarySubstitutionRollbackRemovesNewAuthority(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent-symphony")
	if err := os.WriteFile(path, []byte("new privileged tuple"), 0o440); err != nil {
		t.Fatal(err)
	}
	if err := rollbackSudoers(path, nil, false); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("new sudo authority survived rollback: %v", err)
	}
}

// fakeNoHostIsolation simulates a host where install-host has never been run,
// which is the sole signal that selects the zero-admin default (local) boundary.
func fakeNoHostIsolation(t *testing.T) {
	t.Helper()
	old := hostLookupUser
	hostLookupUser = func(name string) (*user.User, error) {
		if name == workerUser || name == reviewerUser {
			return nil, errors.New("user: unknown user " + name)
		}
		return old(name)
	}
	t.Cleanup(func() { hostLookupUser = old })
}

func TestHostIsolationInstalledReflectsWorkerIdentityLookup(t *testing.T) {
	fakeNoHostIsolation(t)
	if hostIsolationInstalled() {
		t.Fatal("expected host isolation to be reported as not installed")
	}
}

func TestBoundarySelectionUsesLocalModeWhenHostIsolationIsNotInstalled(t *testing.T) {
	fakeNoHostIsolation(t)
	stateRoot := t.TempDir()
	implementation := implementationBoundary(stateRoot)
	if implementation.Command == "sudo" {
		t.Fatalf("expected local boundary, got sudo: %#v", implementation)
	}
	if len(implementation.Args) != 2 || implementation.Args[0] != "agent-host" || implementation.Args[1] != "implementation" {
		t.Fatalf("unexpected local implementation args: %#v", implementation.Args)
	}
	if want := "AGENT_SYMPHONY_LOCAL_ROOT=" + localAttemptRoot(stateRoot); !slices.Contains(implementation.Env, want) {
		t.Fatalf("expected local root env %q, got %#v", want, implementation.Env)
	}
	review := reviewBoundary(stateRoot)
	if review.Command == "sudo" {
		t.Fatalf("expected local review boundary, got sudo: %#v", review)
	}
	if want := "AGENT_SYMPHONY_LOCAL_ROOT=" + localSnapshotRoot(stateRoot); !slices.Contains(review.Env, want) {
		t.Fatalf("expected local snapshot root env %q, got %#v", want, review.Env)
	}
}

func TestAgentHostLocalModeSkipsIdentityCheckAndUsesLocalRoot(t *testing.T) {
	oldExec := hostExecRunner
	t.Cleanup(func() { hostExecRunner = oldExec })
	hostExecRunner = func(_ context.Context, command agentruntime.Command) (agentruntime.Result, error) {
		return agentruntime.Result{Output: strings.Join(command.Env, "|")}, nil
	}
	root := t.TempDir()
	t.Setenv("AGENT_SYMPHONY_LOCAL_ROOT", root)
	payload, _ := json.Marshal(struct {
		Operation string          `json:"operation"`
		Command   boundaryCommand `json:"command"`
	}{"run", boundaryCommand{Name: "git", Args: []string{"-C", root, "rev-parse", "HEAD"}, Dir: root, Env: []string{"MODEL_API_KEY=model-canary", "PATH=/bin"}}})
	var out bytes.Buffer
	if err := agentHost(t.Context(), "implementation", bytes.NewReader(payload), &out); err != nil {
		t.Fatal(err)
	}
	current, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	var result agentruntime.Result
	if err := json.Unmarshal(out.Bytes(), &result); err != nil || !strings.Contains(result.Output, "MODEL_API_KEY=model-canary") || !strings.Contains(result.Output, "HOME="+current.HomeDir) || strings.Contains(result.Output, "GITHUB_TOKEN") {
		t.Fatalf("result=%#v err=%v", result, err)
	}
}

func TestAgentHostLocalModeVerifyProvisionsPrivateRoot(t *testing.T) {
	root := filepath.Join(t.TempDir(), "attempts")
	t.Setenv("AGENT_SYMPHONY_LOCAL_ROOT", root)
	payload, _ := json.Marshal(struct {
		Operation string          `json:"operation"`
		Command   boundaryCommand `json:"command"`
	}{"verify", boundaryCommand{}})
	var out bytes.Buffer
	if err := agentHost(t.Context(), "implementation", bytes.NewReader(payload), &out); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(root)
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 {
		t.Fatalf("local root not provisioned: %v %#v", err, info)
	}
}

func TestHostDiagnosticFallsBackToLocalModeWhenNotInstalled(t *testing.T) {
	fakeNoHostIsolation(t)
	stateRoot := t.TempDir()
	d := hostDiagnostic(stateRoot)
	if d.Status != "pass" || !strings.Contains(d.Message, "zero-admin") {
		t.Fatalf("diagnostic=%#v", d)
	}
	for _, root := range []string{localAttemptRoot(stateRoot), localSnapshotRoot(stateRoot)} {
		info, err := os.Stat(root)
		if err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 {
			t.Fatalf("local root %s not provisioned: %v", root, err)
		}
	}
}

func TestLocalHostDiagnosticRequiresRuntimeStateRoot(t *testing.T) {
	fakeNoHostIsolation(t)
	if d := hostDiagnostic(""); d.Status != "fail" {
		t.Fatalf("expected fail without a runtime state root, got %#v", d)
	}
}

func TestEnsureLocalRootRejectsGroupOrWorldAccessibleDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "attempts")
	if err := os.Mkdir(path, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := ensureLocalRoot(path); err == nil {
		t.Fatal("expected rejection of group-accessible local root")
	}
}

func TestEnsureLocalRootRejectsSymlink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "real")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if err := ensureLocalRoot(link); err == nil {
		t.Fatal("expected rejection of symlinked local root")
	}
}
