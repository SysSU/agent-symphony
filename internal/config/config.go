package config

import (
	"bytes"
	"context"
	"crypto/sha256"
	"debug/elf"
	"debug/macho"
	"debug/pe"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"

	internalgithub "github.com/SysSU/agent-symphony/internal/github"
)

const (
	DefaultPath                          = ".agent-symphony.yaml"
	DefaultReconciliationIntervalSeconds = 60
	ManagedWorkspacePlaceholder          = "{managed_workspace}"
	OrchestratorWorkspacePlaceholder     = "{orchestrator_workspace}"
)

const (
	workspaceTrustConfig = `projects={"{managed_workspace}"={trust_level="untrusted"}}`
	legacyWorkspaceTrust = `projects={"{managed_workspace}"={trust_level="trusted"}}`
	workerPermissions    = `permissions.agent-symphony-worker={filesystem={":root"="deny",":minimal"="read",":tmpdir"="deny",":slash_tmp"="deny",":workspace_roots"={"."="write",".git"="write",".agent-symphony"="write",".codex"="deny",".agents"="deny"}},network={enabled=false}}`
	auditorPermissions   = `permissions.agent-symphony-auditor={filesystem={":root"="deny",":minimal"="read",":tmpdir"="deny",":slash_tmp"="deny",":workspace_roots"={"."="read"}},network={enabled=false}}`
	workerEnvironment    = `shell_environment_policy={inherit="all",include_only=["^(PATH|TMPDIR|XDG_CACHE_HOME|GOCACHE|npm_config_cache|LANG|LC_ALL|TERM|COLORTERM|NO_COLOR|CODEX_HOME|AGENT_SYMPHONY_IMPLEMENTATION_RESULT|AGENT_SYMPHONY_STATUS_REQUEST|AGENT_SYMPHONY_WORKER_GENERATION|AGENT_SYMPHONY_WORKER_LAUNCH_ID|AGENT_SYMPHONY_REVIEW_RESULT)$"]}`
	workerProfileName    = "agent-symphony-worker"
	auditorProfileName   = "agent-symphony-auditor"
)

var workerSafetyArgs = []string{
	"--strict-config",
	"--ask-for-approval", "never",
	"-c", workspaceTrustConfig,
	"-c", `default_permissions="agent-symphony-worker"`,
	"-c", workerPermissions,
	"-c", workerEnvironment,
	"-c", `web_search="disabled"`,
	"--disable", "apps",
	"--disable", "browser_use",
	"--disable", "browser_use_external",
	"--disable", "computer_use",
	"--disable", "hooks",
	"--disable", "image_generation",
	"--disable", "in_app_browser",
	"--disable", "multi_agent",
	"--disable", "multi_agent_v2",
	"--disable", "plugins",
	"--disable", "skill_mcp_dependency_install",
	"--disable", "skill_search",
}

func defaultAuditorCommand() []string {
	return []string{"codex", "--strict-config", "--ask-for-approval", "never", "-c", `projects={"{orchestrator_workspace}"={trust_level="untrusted"}}`, "-c", `default_permissions="agent-symphony-auditor"`, "-c", auditorPermissions, "-c", workerEnvironment, "-c", `web_search="disabled"`, "--disable", "apps", "--disable", "browser_use", "--disable", "browser_use_external", "--disable", "computer_use", "--disable", "hooks", "--disable", "image_generation", "--disable", "in_app_browser", "--disable", "multi_agent", "--disable", "multi_agent_v2", "--disable", "plugins", "--disable", "skill_mcp_dependency_install", "--disable", "skill_search", "exec", "--ignore-user-config", "--ignore-rules", "--sandbox", auditorProfileName, "--skip-git-repo-check", "--ephemeral", "--output-last-message", "{orchestrator_result}", "-"}
}

func defaultWorkerCommand(interactive bool) []string {
	command := append([]string{"codex"}, workerSafetyArgs...)
	if interactive {
		return append(command, "exec", "--ignore-user-config", "--ignore-rules", "--ephemeral")
	}
	return append(command, "exec", "--ignore-user-config", "--ignore-rules", "--ephemeral", "-")
}

// WorkerProfileDigest identifies the exact managed confinement contract.
func WorkerProfileDigest() string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(strings.Join(workerSafetyArgs, "\x00"))))
}

// BindWorkerExecutable resolves and attests the exact Codex binary used by all
// managed workers. The returned digest changes with the path, version, bytes,
// or confinement profile, so a restart cannot trust a differently enforced
// worker generation.
func BindWorkerExecutable(ctx context.Context, commands *Commands) (string, error) {
	if commands == nil {
		return "", errors.New("worker commands are unavailable")
	}
	var canonical, version, binaryDigest string
	for _, command := range []*[]string{&commands.Implementation, &commands.Reviewer} {
		if len(*command) == 0 {
			return "", errors.New("worker command is unavailable")
		}
		path, err := exec.LookPath((*command)[0])
		if err != nil {
			return "", fmt.Errorf("resolve Codex worker executable: %w", err)
		}
		path, err = filepath.Abs(path)
		if err == nil {
			path, err = filepath.EvalSymlinks(path)
		}
		if err == nil {
			path, err = resolveNativeCodex(path)
		}
		if err != nil {
			return "", fmt.Errorf("canonicalize Codex worker executable: %w", err)
		}
		info, err := os.Stat(path)
		if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 || info.Mode().Perm()&0o022 != 0 {
			return "", errors.New("codex worker executable is unsafe")
		}
		file, err := os.Open(path)
		if err != nil {
			return "", err
		}
		hash := sha256.New()
		_, copyErr := io.Copy(hash, file)
		closeErr := file.Close()
		if copyErr != nil || closeErr != nil {
			return "", errors.Join(copyErr, closeErr)
		}
		currentDigest := fmt.Sprintf("%x", hash.Sum(nil))
		output, err := exec.CommandContext(ctx, path, "--version").Output()
		currentVersion := strings.TrimSpace(string(output))
		if err != nil || !strings.HasPrefix(currentVersion, "codex-cli 0.153.") {
			return "", fmt.Errorf("unsupported Codex worker executable version %q", currentVersion)
		}
		if canonical != "" && (canonical != path || version != currentVersion || binaryDigest != currentDigest) {
			return "", errors.New("implementation and reviewer must use the same Codex executable")
		}
		canonical, version, binaryDigest = path, currentVersion, currentDigest
		(*command)[0] = path
	}
	material := strings.Join([]string{WorkerProfileDigest(), canonical, version, binaryDigest}, "\x00")
	return fmt.Sprintf("%x", sha256.Sum256([]byte(material))), nil
}

// PinWorkerExecutable copies the verified Codex installation into the private
// runtime root. Workers execute only this read-only artifact, so replacing the
// configured path after startup cannot change what they run. The production
// caller holds the deployment daemon lock, which serializes publication and
// recovery of an incomplete artifact.
func PinWorkerExecutable(ctx context.Context, stateRoot string, commands *Commands) (string, error) {
	if commands == nil || len(commands.Implementation) == 0 || len(commands.Reviewer) == 0 {
		return "", errors.New("worker commands are unavailable")
	}
	auditSource := ""
	if len(commands.OrchestratorAudit) > 0 {
		if path, err := exec.LookPath(commands.OrchestratorAudit[0]); err == nil {
			if path, err = filepath.Abs(path); err == nil {
				if path, err = filepath.EvalSymlinks(path); err == nil {
					auditSource, _ = resolveNativeCodex(path)
				}
			}
		}
	}
	var source string
	for _, command := range [][]string{commands.Implementation, commands.Reviewer} {
		path, err := exec.LookPath(command[0])
		if err != nil {
			return "", fmt.Errorf("resolve Codex worker executable: %w", err)
		}
		path, err = filepath.Abs(path)
		if err == nil {
			path, err = filepath.EvalSymlinks(path)
		}
		if err == nil {
			path, err = resolveNativeCodex(path)
		}
		info, statErr := os.Stat(path)
		if err != nil || statErr != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 || info.Mode().Perm()&0o022 != 0 || !safeExecutableOwner(info) {
			return "", errors.New("codex worker executable is unsafe")
		}
		if err := validateNativeExecutable(path); err != nil {
			return "", err
		}
		if source != "" && source != path {
			return "", errors.New("implementation and reviewer must use the same Codex executable")
		}
		source = path
	}
	if len(commands.OrchestratorAudit) > 0 && auditSource != source {
		return "", errors.New("orchestrator auditor must use the same Codex executable as managed workers")
	}
	root := source
	if filepath.Base(filepath.Dir(source)) == "bin" {
		candidate := filepath.Dir(filepath.Dir(source))
		if info, err := os.Stat(filepath.Join(candidate, "package.json")); err == nil && info.Mode().IsRegular() {
			root = candidate
		}
	}
	pinRoot := filepath.Join(stateRoot, "worker-executable")
	if err := os.MkdirAll(pinRoot, 0o700); err != nil {
		return "", fmt.Errorf("prepare pinned worker executable: %w", err)
	}
	if err := validatePinnedDirectory(pinRoot, 0o700); err != nil {
		return "", err
	}
	stage, err := os.MkdirTemp(pinRoot, ".pin-")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(stage)
	if err := os.Chmod(stage, 0o700); err != nil {
		return "", err
	}
	treeDigest, relative, err := copyPinnedTree(ctx, root, source, stage)
	if err != nil {
		return "", err
	}
	single := root == source
	target := filepath.Join(pinRoot, treeDigest)
	marker := target + ".ready"
	if err := validatePinnedMarker(marker, treeDigest); errors.Is(err, os.ErrNotExist) {
		if err := os.Rename(stage, target); err != nil {
			if discardErr := discardUnpublishedPinnedTarget(target); discardErr != nil {
				return "", errors.Join(errors.New("pinned worker executable has an unsafe unpublished artifact"), err, discardErr)
			}
			if err := os.Rename(stage, target); err != nil {
				return "", err
			}
		}
		if err := os.Chmod(target, 0o500); err != nil {
			return "", err
		}
		if err := validatePinnedTree(ctx, target, treeDigest, single); err != nil {
			return "", err
		}
		if err := validateNativeExecutable(filepath.Join(target, relative)); err != nil {
			return "", err
		}
		if err := publishPinnedMarker(pinRoot, marker, treeDigest); err != nil {
			return "", err
		}
	} else if err != nil {
		return "", err
	}
	if err := validatePinnedTree(ctx, target, treeDigest, single); err != nil {
		return "", err
	}
	pinned := filepath.Join(target, relative)
	if err := validateNativeExecutable(pinned); err != nil {
		return "", err
	}
	commands.Implementation[0], commands.Reviewer[0] = pinned, pinned
	digest, err := BindWorkerExecutable(ctx, commands)
	if err != nil {
		return "", err
	}
	if len(commands.OrchestratorAudit) > 0 && auditSource == source {
		commands.OrchestratorAudit[0] = commands.Implementation[0]
	}
	return digest, nil
}

func publishPinnedMarker(root, path, digest string) error {
	file, err := os.CreateTemp(root, ".ready-")
	if err != nil {
		return err
	}
	name := file.Name()
	defer os.Remove(name)
	if _, err = io.WriteString(file, digest+"\n"); err == nil {
		err = file.Sync()
	}
	if err == nil {
		err = file.Chmod(0o400)
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(name, path)
	}
	if err == nil {
		directory, openErr := os.Open(root)
		if openErr == nil {
			openErr = directory.Sync()
			if closeErr := directory.Close(); openErr == nil {
				openErr = closeErr
			}
		}
		err = openErr
	}
	return err
}

func discardUnpublishedPinnedTarget(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || !safeExecutableOwner(info) {
		return errors.New("unpublished pinned worker target is unsafe")
	}
	err = filepath.WalkDir(path, func(candidate string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		info, err := os.Lstat(candidate)
		if err != nil || info.Mode()&os.ModeSymlink != 0 || !safeExecutableOwner(info) || !entry.IsDir() && !info.Mode().IsRegular() {
			return errors.New("unpublished pinned worker target contains an unsafe entry")
		}
		if entry.IsDir() {
			return os.Chmod(candidate, 0o700)
		}
		return nil
	})
	if err != nil {
		return err
	}
	return os.RemoveAll(path)
}

func validatePinnedMarker(path, digest string) error {
	listed, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !listed.Mode().IsRegular() || listed.Mode().Perm() != 0o400 || !safeExecutableOwner(listed) {
		return errors.New("pinned worker executable marker is unsafe")
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	opened, statErr := file.Stat()
	body, readErr := io.ReadAll(io.LimitReader(file, 66))
	closeErr := file.Close()
	if statErr != nil || !os.SameFile(listed, opened) || readErr != nil || closeErr != nil || string(body) != digest+"\n" {
		return errors.New("pinned worker executable marker is invalid")
	}
	return nil
}

func resolveNativeCodex(path string) (string, error) {
	if filepath.Base(path) != "codex.js" {
		return path, nil
	}
	platform := map[string]string{
		"darwin/amd64": "darwin-x64", "darwin/arm64": "darwin-arm64",
		"linux/amd64": "linux-x64", "linux/arm64": "linux-arm64",
		"windows/amd64": "win32-x64", "windows/arm64": "win32-arm64",
	}[runtime.GOOS+"/"+runtime.GOARCH]
	if platform == "" {
		return "", errors.New("codex npm wrapper has no supported native worker executable")
	}
	packageRoot := filepath.Dir(filepath.Dir(path))
	matches, err := filepath.Glob(filepath.Join(packageRoot, "node_modules", "@openai", "codex-"+platform, "vendor", "*", "bin", "codex"))
	if err != nil || len(matches) != 1 {
		return "", errors.New("codex npm wrapper has no exact native worker executable")
	}
	return filepath.EvalSymlinks(matches[0])
}

func validateNativeExecutable(path string) error {
	if testNativeWorkerExecutableAllowed(path) {
		return nil
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	switch runtime.GOOS {
	case "linux":
		_, err = elf.NewFile(file)
	case "darwin":
		if _, err = macho.NewFile(file); err != nil {
			_, err = macho.NewFatFile(file)
		}
	case "windows":
		_, err = pe.NewFile(file)
	default:
		err = fmt.Errorf("unsupported platform %s", runtime.GOOS)
	}
	if err != nil {
		return fmt.Errorf("codex worker executable is not a native %s executable: %w", runtime.GOOS, err)
	}
	return nil
}

// The production build has no configurable escape hatch for script workers.
// A build-tagged full-system fixture replaces this hook in test binaries only.
var testNativeWorkerExecutableAllowed = func(string) bool { return false }

func copyPinnedTree(ctx context.Context, root, executable, destination string) (string, string, error) {
	single := root == executable
	relative := filepath.Base(executable)
	if !single {
		var err error
		relative, err = filepath.Rel(root, executable)
		if err != nil || relative == "." || strings.HasPrefix(relative, "..") {
			return "", "", errors.New("codex worker executable escaped its installation")
		}
	}
	hash := sha256.New()
	var directories []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		rel := relative
		if !single {
			var err error
			rel, err = filepath.Rel(root, path)
			if err != nil || strings.HasPrefix(rel, "..") {
				return errors.New("codex worker installation escaped its root")
			}
		}
		info, err := os.Lstat(path)
		if err != nil || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o022 != 0 || !safeExecutableOwner(info) {
			return errors.New("codex worker installation is unsafe")
		}
		if !entry.IsDir() && !info.Mode().IsRegular() {
			return errors.New("codex worker installation contains a special file")
		}
		_, _ = io.WriteString(hash, rel+"\x00")
		target := filepath.Join(destination, rel)
		if entry.IsDir() {
			if rel != "." {
				if err := os.Mkdir(target, 0o700); err != nil {
					return err
				}
			}
			directories = append(directories, target)
			return nil
		}
		input, err := os.Open(path)
		if err != nil {
			return err
		}
		opened, err := input.Stat()
		if err != nil || !os.SameFile(info, opened) {
			_ = input.Close()
			return errors.New("codex worker installation changed while opening")
		}
		output, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err == nil {
			_, err = io.Copy(io.MultiWriter(output, hash), input)
		}
		closeInput, closeOutput := input.Close(), error(nil)
		if output != nil {
			closeOutput = output.Close()
		}
		if err = errors.Join(err, closeInput, closeOutput); err != nil {
			return err
		}
		mode := os.FileMode(0o400)
		if info.Mode().Perm()&0o111 != 0 {
			mode = 0o500
		}
		return os.Chmod(target, mode)
	})
	if err != nil {
		return "", "", err
	}
	for index := len(directories) - 1; index >= 0; index-- {
		if err := os.Chmod(directories[index], 0o500); err != nil {
			return "", "", err
		}
	}
	return fmt.Sprintf("%x", hash.Sum(nil)), relative, nil
}

func validatePinnedDirectory(path string, mode os.FileMode) error {
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != mode || !safeExecutableOwner(info) {
		return errors.New("pinned worker executable directory is unsafe")
	}
	return nil
}

func validatePinnedTree(ctx context.Context, root, expected string, single bool) error {
	hash := sha256.New()
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil || strings.HasPrefix(rel, "..") {
			return errors.New("pinned worker executable escaped its root")
		}
		info, err := os.Lstat(path)
		if err != nil || info.Mode()&os.ModeSymlink != 0 || !safeExecutableOwner(info) {
			return errors.New("pinned worker executable is unsafe")
		}
		if entry.IsDir() {
			if info.Mode().Perm() != 0o500 {
				return errors.New("pinned worker executable directory is writable")
			}
			if !(single && rel == ".") {
				_, _ = io.WriteString(hash, rel+"\x00")
			}
			return nil
		}
		if !info.Mode().IsRegular() || info.Mode().Perm() != 0o400 && info.Mode().Perm() != 0o500 {
			return errors.New("pinned worker executable contains an unsafe file")
		}
		_, _ = io.WriteString(hash, rel+"\x00")
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		opened, statErr := file.Stat()
		if statErr != nil || !os.SameFile(info, opened) {
			_ = file.Close()
			return errors.New("pinned worker executable changed while opening")
		}
		_, copyErr := io.Copy(hash, file)
		return errors.Join(copyErr, file.Close())
	})
	if err != nil {
		return err
	}
	if fmt.Sprintf("%x", hash.Sum(nil)) != expected {
		return errors.New("pinned worker executable digest does not match its path")
	}
	return nil
}

func VerifyWorkerExecutable(ctx context.Context, path, expectedDigest string) error {
	commands := Default("worker/verification").Commands
	commands.Implementation[0], commands.Reviewer[0] = path, path
	digest, err := BindWorkerExecutable(ctx, &commands)
	if err != nil || digest != expectedDigest || commands.Implementation[0] != path {
		return errors.Join(errors.New("codex worker executable identity changed"), err)
	}
	return nil
}

// WorkerSandboxArgs runs a deterministic capability probe under the same profile.
func WorkerSandboxArgs(workspace string, command ...string) []string {
	args := []string{"sandbox", "-c", workerPermissions, "-P", workerProfileName, "-C", workspace, "--"}
	return append(args, command...)
}

// WorkerSandboxArgsForExecutable makes the attested Codex installation
// readable when it lives outside the platform's minimal system roots.
func WorkerSandboxArgsForExecutable(workspace, codexExecutable string, command ...string) []string {
	installRoot := filepath.Dir(filepath.Dir(filepath.Clean(codexExecutable)))
	args := []string{"sandbox", "-c", workerPermissions, "-P", workerProfileName, "-C", workspace, "--sandbox-state-readable-root", installRoot, "--"}
	return append(args, command...)
}

type Config struct {
	Version                       int                `json:"version"`
	Repository                    string             `json:"repository"`
	Labels                        Labels             `json:"labels"`
	Dependencies                  Dependencies       `json:"dependencies"`
	CompletionPolicies            CompletionPolicies `json:"completion_policies"`
	Concurrency                   int                `json:"concurrency"`
	ReconciliationIntervalSeconds int                `json:"reconciliation_interval_seconds"`
	WorktreeRoot                  string             `json:"worktree_root"`
	DocsPaths                     []string           `json:"docs_paths"`
	Commands                      Commands           `json:"commands"`
	Status                        Status             `json:"status"`
}

type Labels struct {
	Ready       string `json:"ready"`
	PriorityP1  string `json:"priority_p1"`
	PriorityP2  string `json:"priority_p2"`
	PriorityP3  string `json:"priority_p3"`
	IssueFilter string `json:"issue_filter,omitempty"`
}

type Dependencies struct {
	Section string `json:"section"`
}

type CompletionPolicies struct {
	Default         string `json:"default"`
	HumanReview     string `json:"human_review_label"`
	AutonomousMerge string `json:"autonomous_merge_label"`
}

type Commands struct {
	Implementation    []string `json:"implementation"`
	Reviewer          []string `json:"reviewer"`
	Orchestrator      []string `json:"orchestrator,omitempty"`
	OrchestratorAudit []string `json:"orchestrator_audit,omitempty"`
	Environment       []string `json:"environment_allowlist"`
}

type Status struct {
	Format string `json:"format"`
	Color  string `json:"color"`
}

func Default(repository string) Config {
	return Config{
		Version:    1,
		Repository: repository,
		Labels: Labels{
			Ready: "agent-ready", PriorityP1: "priority:P1", PriorityP2: "priority:P2", PriorityP3: "priority:P3",
		},
		Dependencies: Dependencies{Section: "Dependencies"},
		CompletionPolicies: CompletionPolicies{
			Default: "human-review", HumanReview: "needs-human-review", AutonomousMerge: "autonomous-merge",
		},
		Concurrency:                   1,
		ReconciliationIntervalSeconds: DefaultReconciliationIntervalSeconds,
		WorktreeRoot:                  ".worktrees",
		DocsPaths:                     []string{"README.md", "docs"},
		Commands: Commands{
			Implementation: defaultWorkerCommand(false), Reviewer: defaultWorkerCommand(true),
			Orchestrator:      []string{"codex", "-c", `projects={"{orchestrator_workspace}"={trust_level="trusted"}}`, "--sandbox", "danger-full-access", "--ask-for-approval", "never", "--no-alt-screen"},
			OrchestratorAudit: defaultAuditorCommand(),
			Environment:       []string{"LANG", "LC_ALL", "PATH", "TERM"},
		},
		Status: Status{Format: "human", Color: "auto"},
	}
}

func Load(path string) (Config, error) {
	root, err := GitRoot()
	if err != nil {
		return Config{}, err
	}
	return load(path, root)
}

func load(path, root string) (Config, error) {
	path, root, err := resolveLocation(path, root)
	if err != nil {
		return Config{}, err
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	if err := rejectDuplicateKeys(b); err != nil {
		return Config{}, fmt.Errorf("parse %s: %w", path, err)
	}
	var raw any
	if err := json.Unmarshal(b, &raw); err != nil {
		return Config{}, fmt.Errorf("parse %s: configuration uses the JSON subset of YAML: %w", path, err)
	}
	if key := secretKey(raw); key != "" {
		return Config{}, fmt.Errorf("secret-shaped key %q is forbidden; keep credentials outside repository configuration", key)
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	var c Config
	if fields, ok := raw.(map[string]any); ok {
		if interval, present := fields["reconciliation_interval_seconds"]; !present {
			c.ReconciliationIntervalSeconds = DefaultReconciliationIntervalSeconds
		} else if interval == nil {
			return Config{}, fmt.Errorf("parse %s: reconciliation_interval_seconds must be between 1 and 60", path)
		}
	}
	if err := dec.Decode(&c); err != nil {
		return Config{}, fmt.Errorf("parse %s: %w", path, err)
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return Config{}, fmt.Errorf("parse %s: multiple JSON values", path)
	}
	normalizeLegacyCodexCommand(&c)
	if err := c.Validate(); err != nil {
		return Config{}, err
	}
	for _, candidate := range append([]string{c.WorktreeRoot}, c.DocsPaths...) {
		if err := containedPath(root, candidate); err != nil {
			return Config{}, err
		}
	}
	return c, nil
}

func normalizeLegacyCodexCommand(c *Config) {
	defaults := Default(c.Repository).Commands
	c.Commands.Implementation = upgradeCodexCommand(c.Commands.Implementation, defaults.Implementation,
		[]string{"exec", "--dangerously-bypass-approvals-and-sandbox"},
		[]string{"exec", "--dangerously-bypass-approvals-and-sandbox", "-"},
		[]string{"exec", "-c", legacyWorkspaceTrust, "--dangerously-bypass-approvals-and-sandbox", "-"})
	c.Commands.Reviewer = upgradeCodexCommand(c.Commands.Reviewer, defaults.Reviewer,
		[]string{"exec", "--dangerously-bypass-approvals-and-sandbox", "-"},
		[]string{"--dangerously-bypass-approvals-and-sandbox", "--no-alt-screen"},
		[]string{"-c", legacyWorkspaceTrust, "--dangerously-bypass-approvals-and-sandbox", "--no-alt-screen"})
	c.Commands.OrchestratorAudit = upgradeCodexCommand(c.Commands.OrchestratorAudit, defaults.OrchestratorAudit,
		[]string{"--ask-for-approval", "never", "exec", "-c", `projects={"{orchestrator_workspace}"={trust_level="trusted"}}`, "-c", `model_reasoning_effort="medium"`, "--sandbox", "danger-full-access", "--skip-git-repo-check", "--ephemeral", "--output-last-message", "{orchestrator_result}", "-"},
		[]string{"exec", "-c", `projects={"{orchestrator_workspace}"={trust_level="trusted"}}`, "-c", `model_reasoning_effort="medium"`, "--sandbox", "danger-full-access", "--skip-git-repo-check", "--ephemeral", "--output-last-message", "{orchestrator_result}", "-"})
}

func upgradeCodexCommand(command, replacement []string, legacy ...[]string) []string {
	if len(command) > 0 && filepath.Base(command[0]) == "codex" {
		for _, args := range legacy {
			if slices.Equal(command[1:], args) {
				upgraded := slices.Clone(replacement)
				upgraded[0] = command[0]
				return upgraded
			}
		}
	}
	return command
}

// ExpandManagedWorkspace binds a configured command to one already validated
// absolute runtime workspace without changing global Codex trust state.
func ExpandManagedWorkspace(command []string, workspace string) ([]string, error) {
	if !filepath.IsAbs(workspace) || filepath.Clean(workspace) != workspace || strings.ContainsRune(workspace, 0) {
		return nil, errors.New("managed workspace must be a clean absolute path")
	}
	quoted, err := json.Marshal(workspace)
	if err != nil {
		return nil, err
	}
	escaped := string(quoted[1 : len(quoted)-1])
	expanded := append([]string(nil), command...)
	for index := range expanded {
		expanded[index] = strings.ReplaceAll(expanded[index], ManagedWorkspacePlaceholder, escaped)
		expanded[index] = strings.ReplaceAll(expanded[index], OrchestratorWorkspacePlaceholder, escaped)
	}
	return expanded, nil
}

func GitRoot() (string, error) {
	out, err := exec.Command("git", "rev-parse", "--show-toplevel").CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("resolve Git root: %s", strings.TrimSpace(string(out)))
	}
	root, err := filepath.EvalSymlinks(strings.TrimSpace(string(out)))
	if err != nil {
		return "", fmt.Errorf("resolve Git root: %w", err)
	}
	return root, nil
}

func ValidateLocation(path string) (string, error) {
	root, err := GitRoot()
	if err != nil {
		return "", err
	}
	_, root, err = resolveLocation(path, root)
	return root, err
}

func resolveLocation(path, root string) (string, string, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return "", "", err
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		return "", "", err
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", "", err
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(abs))
	if err != nil {
		return "", "", fmt.Errorf("resolve config path: %w", err)
	}
	abs = filepath.Join(parent, filepath.Base(abs))
	if resolved, err := filepath.EvalSymlinks(abs); err == nil {
		abs = resolved
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", "", fmt.Errorf("resolve config path: %w", err)
	}
	rel, err := filepath.Rel(root, abs)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", "", fmt.Errorf("config path %q must be inside resolved Git root %q", path, root)
	}
	return abs, root, nil
}

func Write(path string, c Config) error {
	b, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return os.WriteFile(path, b, 0o644)
}

func (c Config) Validate() error {
	var problems []string
	if c.Version != 1 {
		problems = append(problems, "version must be 1")
	}
	parts := strings.Split(c.Repository, "/")
	if len(parts) != 2 || !repositoryPart(parts[0]) || !repositoryPart(parts[1]) {
		problems = append(problems, "repository must be owner/name")
	}
	labels := []string{c.Labels.Ready, c.Labels.PriorityP1, c.Labels.PriorityP2, c.Labels.PriorityP3, c.CompletionPolicies.HumanReview, c.CompletionPolicies.AutonomousMerge, internalgithub.NeedsAttentionLabel}
	if c.Labels.IssueFilter != "" {
		labels = append(labels, c.Labels.IssueFilter)
	}
	seen := map[string]bool{}
	for _, label := range labels {
		if strings.TrimSpace(label) == "" {
			problems = append(problems, "labels must not be empty")
			break
		}
		identity := strings.ToLower(label)
		if seen[identity] {
			problems = append(problems, fmt.Sprintf("label %q is used more than once", label))
		}
		seen[identity] = true
	}
	if c.Dependencies.Section == "" {
		problems = append(problems, "dependencies.section must not be empty")
	}
	if c.CompletionPolicies.Default != "human-review" {
		problems = append(problems, "completion_policies.default must be human-review")
	}
	if c.Concurrency < 1 {
		problems = append(problems, "concurrency must be at least 1")
	}
	if c.ReconciliationIntervalSeconds < 1 || c.ReconciliationIntervalSeconds > 60 {
		problems = append(problems, "reconciliation_interval_seconds must be between 1 and 60")
	}
	if err := safePath(c.WorktreeRoot); err != nil {
		problems = append(problems, "worktree_root: "+err.Error())
	}
	if len(c.DocsPaths) == 0 {
		problems = append(problems, "docs_paths must not be empty")
	}
	for _, path := range c.DocsPaths {
		if err := safePath(path); err != nil {
			problems = append(problems, "docs_paths: "+err.Error())
		}
	}
	for name, command := range map[string][]string{"implementation": c.Commands.Implementation, "reviewer": c.Commands.Reviewer} {
		if len(command) == 0 || strings.TrimSpace(command[0]) == "" {
			problems = append(problems, "commands."+name+" must contain an executable")
		} else if !validWorkerCommand(command, name == "reviewer") {
			problems = append(problems, "commands."+name+" must use the managed rootless Codex worker profile")
		}
		for _, arg := range command {
			if strings.ContainsRune(arg, 0) || strings.ContainsAny(arg, "\r\n") {
				problems = append(problems, "commands."+name+" contains an unsafe argument")
			}
			if secretName(arg) != "" {
				problems = append(problems, "commands."+name+" contains a credential-shaped argument")
			}
		}
	}
	for _, optional := range []struct {
		name    string
		command []string
	}{{"orchestrator", c.Commands.Orchestrator}, {"orchestrator_audit", c.Commands.OrchestratorAudit}} {
		if optional.command == nil {
			continue
		}
		if len(optional.command) == 0 || strings.TrimSpace(optional.command[0]) == "" {
			problems = append(problems, "commands."+optional.name+" must contain an executable when configured")
		}
		for _, arg := range optional.command {
			if strings.ContainsRune(arg, 0) || strings.ContainsAny(arg, "\r\n") {
				problems = append(problems, "commands."+optional.name+" contains an unsafe argument")
			}
			if secretName(arg) != "" {
				problems = append(problems, "commands."+optional.name+" contains a credential-shaped argument")
			}
		}
	}
	if c.Commands.OrchestratorAudit != nil && !validAuditorCommand(c.Commands.OrchestratorAudit) {
		problems = append(problems, "commands.orchestrator_audit must use the managed rootless read-only Codex auditor profile")
	}
	for _, name := range c.Commands.Environment {
		if !environmentName(name) {
			problems = append(problems, "commands.environment_allowlist contains an invalid variable name")
		}
		if forbiddenAgentEnvironment(name) {
			problems = append(problems, fmt.Sprintf("commands.environment_allowlist contains forbidden credential variable %q", name))
		}
		if name == "TMPDIR" || name == "XDG_CACHE_HOME" || name == "GOCACHE" || name == "npm_config_cache" {
			problems = append(problems, fmt.Sprintf("commands.environment_allowlist contains worker-managed path %q", name))
		}
	}
	if c.Status.Format != "human" && c.Status.Format != "json" {
		problems = append(problems, "status.format must be human or json")
	}
	if c.Status.Color != "auto" && c.Status.Color != "always" && c.Status.Color != "never" {
		problems = append(problems, "status.color must be auto, always, or never")
	}
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "; "))
	}
	return nil
}

func validWorkerCommand(command []string, interactive bool) bool {
	if len(command) == 0 || filepath.Base(command[0]) != "codex" {
		return false
	}
	want := defaultWorkerCommand(interactive)
	return slices.Equal(command[1:], want[1:])
}

func validAuditorCommand(command []string) bool {
	if len(command) == 0 || filepath.Base(command[0]) != "codex" {
		return false
	}
	want := defaultAuditorCommand()
	return slices.Equal(command[1:], want[1:])
}

func environmentName(name string) bool {
	if name == "" || !(name[0] == '_' || name[0] >= 'A' && name[0] <= 'Z') {
		return false
	}
	for i := 1; i < len(name); i++ {
		if !(name[i] == '_' || name[i] >= 'A' && name[i] <= 'Z' || name[i] >= '0' && name[i] <= '9') {
			return false
		}
	}
	return true
}

func forbiddenAgentEnvironment(name string) bool {
	_, err := internalgithub.WorkerEnvironmentWith(nil, name)
	return err != nil
}

func repositoryPart(part string) bool {
	if part == "" || part == "." || part == ".." {
		return false
	}
	for _, r := range part {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.') {
			return false
		}
	}
	return true
}

func safePath(path string) error {
	if path == "" || filepath.IsAbs(path) || filepath.Clean(path) != path || path == "." || path == ".." || strings.HasPrefix(path, ".."+string(filepath.Separator)) {
		return fmt.Errorf("%q must be a clean repository-relative path", path)
	}
	first := strings.Split(filepath.ToSlash(path), "/")[0]
	if first == ".git" || first == DefaultPath {
		return fmt.Errorf("%q targets protected repository metadata", path)
	}
	return nil
}

func containedPath(root, path string) error {
	root, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		return err
	}
	candidate := filepath.Join(root, path)
	for {
		resolved, err := filepath.EvalSymlinks(candidate)
		if err == nil {
			rel, err := filepath.Rel(root, resolved)
			if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
				return fmt.Errorf("path %q escapes the repository through a symbolic link", path)
			}
			return nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("inspect path %q: %w", path, err)
		}
		parent := filepath.Dir(candidate)
		if parent == candidate {
			return err
		}
		candidate = parent
	}
}

func secretKey(v any) string {
	var walk func(any) string
	walk = func(value any) string {
		switch value := value.(type) {
		case map[string]any:
			for key, child := range value {
				if secretName(key) != "" {
					return key
				}
				if found := walk(child); found != "" {
					return found
				}
			}
		case []any:
			for _, child := range value {
				if found := walk(child); found != "" {
					return found
				}
			}
		}
		return ""
	}
	return walk(v)
}

func secretName(value string) string {
	lower := strings.ToLower(value)
	for _, word := range []string{"token", "secret", "password", "passwd", "private_key", "private-key", "credential", "api_key", "api-key", "authorization", "github_pat"} {
		if strings.Contains(lower, word) {
			return word
		}
	}
	return ""
}

func rejectDuplicateKeys(b []byte) error {
	dec := json.NewDecoder(bytes.NewReader(b))
	var value func() error
	value = func() error {
		token, err := dec.Token()
		if err != nil {
			return err
		}
		delim, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		switch delim {
		case '{':
			seen := map[string]bool{}
			for dec.More() {
				keyToken, err := dec.Token()
				if err != nil {
					return err
				}
				key := keyToken.(string)
				if seen[key] {
					return fmt.Errorf("duplicate key %q", key)
				}
				seen[key] = true
				if err := value(); err != nil {
					return err
				}
			}
		case '[':
			for dec.More() {
				if err := value(); err != nil {
					return err
				}
			}
		}
		_, err = dec.Token()
		return err
	}
	return value()
}
