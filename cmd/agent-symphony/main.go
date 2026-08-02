package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/SysSU/agent-symphony/internal/config"
	internalgithub "github.com/SysSU/agent-symphony/internal/github"
)

const outputVersion = 1

var (
	githubAPI    = "https://api.github.com"
	githubClient = http.DefaultClient
)

type diagnostic struct {
	Name    string `json:"name"`
	Status  string `json:"status"`
	Message string `json:"message"`
	Action  string `json:"action,omitempty"`
}

type envelope struct {
	Version     int          `json:"version"`
	Command     string       `json:"command"`
	OK          bool         `json:"ok"`
	Data        any          `json:"data,omitempty"`
	Diagnostics []diagnostic `json:"diagnostics,omitempty"`
	Error       string       `json:"error,omitempty"`
}

type environmentToken string

func (t environmentToken) Token(context.Context) (internalgithub.InstallationToken, error) {
	if t == "" {
		return internalgithub.InstallationToken{}, errors.New("GITHUB_TOKEN is required")
	}
	return internalgithub.InstallationToken{Value: string(t), ExpiresAt: time.Now().Add(time.Hour)}, nil
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		usage(stdout)
		return 0
	}
	command := args[0]
	wantsJSON := hasJSONFlag(args[1:])
	if command == "help" || command == "--help" || command == "-h" {
		if len(args) != 1 {
			return misuse(stderr, wantsJSON, command, "help accepts no arguments")
		}
		usage(stdout)
		return 0
	}
	subcommand := ""
	flagArgs := args[1:]
	if command == "config" && len(flagArgs) > 0 && flagArgs[0] == "view" {
		subcommand = "view"
		flagArgs = flagArgs[1:]
	}
	fs := flag.NewFlagSet(command, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	path := fs.String("config", config.DefaultPath, "configuration path")
	jsonOutput := fs.Bool("json", false, "emit versioned JSON")
	statePath := fs.String("state", "", "pull-request recovery state file")
	if err := fs.Parse(flagArgs); err != nil {
		return misuse(stderr, wantsJSON, command, err.Error())
	}

	switch command {
	case "init":
		if fs.NArg() != 0 {
			return misuse(stderr, wantsJSON, command, "init accepts no positional arguments")
		}
		if _, err := config.ValidateLocation(*path); err != nil {
			return fail(stderr, *jsonOutput, command, err.Error())
		}
		if _, err := os.Stat(*path); err == nil {
			return fail(stderr, *jsonOutput, command, *path+" already exists")
		} else if !errors.Is(err, os.ErrNotExist) {
			return fail(stderr, *jsonOutput, command, err.Error())
		}
		repository, err := repositoryFromGit()
		if err != nil {
			return fail(stderr, *jsonOutput, command, "cannot determine GitHub repository: "+err.Error())
		}
		if err := config.Write(*path, config.Default(repository)); err != nil {
			return fail(stderr, *jsonOutput, command, err.Error())
		}
		return success(stdout, *jsonOutput, command, map[string]string{"path": *path}, "created "+*path)

	case "validate":
		if fs.NArg() != 0 {
			return misuse(stderr, wantsJSON, command, "validate accepts no positional arguments")
		}
		c, err := config.Load(*path)
		if err != nil {
			return fail(stderr, *jsonOutput, command, err.Error())
		}
		return success(stdout, *jsonOutput, command, c, *path+" is valid")

	case "config":
		if subcommand != "view" || fs.NArg() != 0 {
			return misuse(stderr, wantsJSON, command, "usage: agent-symphony config view [--config path] [--json]")
		}
		c, err := config.Load(*path)
		if err != nil {
			return fail(stderr, *jsonOutput, command, err.Error())
		}
		if *jsonOutput {
			return writeJSON(stdout, envelope{Version: outputVersion, Command: "config view", OK: true, Data: c})
		}
		b, _ := json.MarshalIndent(c, "", "  ")
		fmt.Fprintln(stdout, string(b))
		return 0

	case "pr-governance":
		if fs.NArg() != 0 {
			return misuse(stderr, wantsJSON, command, "pr-governance accepts no positional arguments")
		}
		if *statePath == "" {
			return fail(stderr, *jsonOutput, command, "--state is required")
		}
		if info, err := os.Lstat(*statePath); err != nil || !info.Mode().IsRegular() {
			return fail(stderr, *jsonOutput, command, "--state must name an existing regular recovery file")
		}
		c, err := config.Load(*path)
		if err != nil {
			return fail(stderr, *jsonOutput, command, err.Error())
		}
		appID, err := positiveEnvironmentInt64("AGENT_SYMPHONY_GITHUB_APP_ID")
		if err != nil {
			return fail(stderr, *jsonOutput, command, err.Error())
		}
		actorID, err := positiveEnvironmentInt64("AGENT_SYMPHONY_GITHUB_APP_ACTOR_ID")
		if err != nil {
			return fail(stderr, *jsonOutput, command, err.Error())
		}
		actor := int(actorID)
		if int64(actor) != actorID {
			return fail(stderr, *jsonOutput, command, "AGENT_SYMPHONY_GITHUB_APP_ACTOR_ID is out of range")
		}
		token := os.Getenv("GITHUB_TOKEN")
		if token == "" {
			return fail(stderr, *jsonOutput, command, "GITHUB_TOKEN is required")
		}
		api := internalgithub.API{BaseURL: githubAPI, Tokens: environmentToken(token), HTTP: githubClient}
		prConfig := internalgithub.PRAdapterConfig{
			Repository: c.Repository, ReadyLabel: c.Labels.Ready, HumanReviewLabel: c.CompletionPolicies.HumanReview,
			AutonomousMergeLabel: c.CompletionPolicies.AutonomousMerge, MergeMethod: "squash", PriorityP1Label: c.Labels.PriorityP1,
			PriorityP2Label: c.Labels.PriorityP2, PriorityP3Label: c.Labels.PriorityP3, DependencySection: c.Dependencies.Section,
			DefaultCompletion: c.CompletionPolicies.Default, ApprovalCommand: "/agent-symphony approve", CancelCommand: "/agent-symphony cancel",
			RetryCommand: "/agent-symphony retry", AppID: appID, AppActorID: actor,
		}
		if err := internalgithub.RunPRReconciliation(context.Background(), api, prConfig, *statePath); err != nil {
			return fail(stderr, *jsonOutput, command, internalgithub.Redact(err.Error()))
		}
		return success(stdout, *jsonOutput, command, map[string]string{"state": *statePath}, "pull-request governance reconciliation complete")

	case "doctor", "diagnostics":
		if fs.NArg() != 0 {
			return misuse(stderr, wantsJSON, command, command+" accepts no positional arguments")
		}
		c, err := config.Load(*path)
		if err != nil {
			return fail(stderr, *jsonOutput, command, err.Error())
		}
		diagnostics := doctor(c)
		ok := true
		for _, d := range diagnostics {
			if d.Status == "fail" {
				ok = false
			}
		}
		if *jsonOutput {
			code := writeJSON(stdout, envelope{Version: outputVersion, Command: command, OK: ok, Diagnostics: diagnostics})
			if !ok {
				return 1
			}
			return code
		}
		for _, d := range diagnostics {
			fmt.Fprintf(stdout, "%-5s %-14s %s\n", strings.ToUpper(d.Status), d.Name, d.Message)
			if d.Action != "" {
				fmt.Fprintln(stdout, "      action: "+d.Action)
			}
		}
		if !ok {
			return 1
		}
		return 0
	default:
		return misuse(stderr, wantsJSON, command, fmt.Sprintf("unknown command %q", command))
	}
}

func positiveEnvironmentInt64(name string) (int64, error) {
	value, err := strconv.ParseInt(os.Getenv(name), 10, 64)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return value, nil
}

func doctor(c config.Config) []diagnostic {
	var result []diagnostic
	if runtime.GOOS == "darwin" || runtime.GOOS == "linux" {
		result = append(result, diagnostic{"platform", "pass", runtime.GOOS + "/" + runtime.GOARCH, ""})
	} else {
		result = append(result, diagnostic{"platform", "fail", runtime.GOOS + " is unsupported", "Use macOS, Linux, or WSL2."})
	}
	if runtime.GOOS == "linux" && isWSL() {
		root, rootErr := config.GitRoot()
		filesystem, mountErr := mountedFilesystem(root, "/proc/mounts")
		if rootErr != nil || mountErr != nil {
			message := firstNonempty(errorText(rootErr), errorText(mountErr))
			result = append(result, diagnostic{"wsl-filesystem", "fail", message, "Run inside a Git repository on the WSL Linux filesystem."})
		} else if filesystem == "drvfs" || filesystem == "9p" {
			result = append(result, diagnostic{"wsl-filesystem", "fail", "repository is on a Windows-mounted filesystem", "Move the repository into the WSL Linux filesystem."})
		} else {
			result = append(result, diagnostic{"wsl-filesystem", "pass", "Git root is on " + filesystem, ""})
		}
	}
	result = append(result, executableDiagnostic("git", []string{"--version"}))
	result = append(result, tmuxDiagnostic())
	for _, configured := range []struct {
		name    string
		command []string
	}{{"implementation", c.Commands.Implementation}, {"reviewer", c.Commands.Reviewer}} {
		name, command := configured.name, configured.command
		if path, err := exec.LookPath(command[0]); err != nil {
			result = append(result, diagnostic{name + " command", "fail", command[0] + " was not found", "Install it or update commands." + name + "."})
		} else {
			result = append(result, diagnostic{name + " command", "pass", path, ""})
		}
	}
	if out, err := exec.Command("git", "rev-parse", "--show-toplevel").CombinedOutput(); err != nil {
		result = append(result, diagnostic{"repository", "fail", strings.TrimSpace(string(out)), "Run doctor inside the configured Git repository."})
	} else {
		result = append(result, diagnostic{"repository", "pass", strings.TrimSpace(string(out)), ""})
	}
	if repository, err := repositoryFromGit(); err != nil {
		result = append(result, diagnostic{"repository identity", "fail", err.Error(), "Configure origin as the GitHub repository named in configuration."})
	} else if repository != c.Repository {
		result = append(result, diagnostic{"repository identity", "fail", "origin is " + repository + ", configuration names " + c.Repository, "Run init in the intended repository or correct repository."})
	} else {
		result = append(result, diagnostic{"repository identity", "pass", repository, ""})
	}
	result = append(result, githubDiagnostics(c.Repository)...)
	result = append(result, diagnostic{"host isolation", "warn", "worker/reviewer accounts, roots, sudo rules, and tmux canaries are not installed by issue #6", "A downstream install-host implementation must prove these boundaries before serve accepts work."})
	result = append(result, diagnostic{"GitHub policy", "warn", "App webhook events, required policy check, and merge permissions are not configured by issue #6", "Complete GitHub App setup in the downstream integration issue before serving work."})
	return result
}

func executableDiagnostic(name string, args []string) diagnostic {
	path, err := exec.LookPath(name)
	if err != nil {
		return diagnostic{name, "fail", name + " was not found", "Install " + name + " and ensure it is on PATH."}
	}
	out, err := exec.Command(path, args...).CombinedOutput()
	if err != nil {
		return diagnostic{name, "fail", strings.TrimSpace(string(out)), "Repair the " + name + " installation."}
	}
	return diagnostic{name, "pass", strings.TrimSpace(string(out)), ""}
}

func tmuxDiagnostic() diagnostic {
	path, err := exec.LookPath("tmux")
	if err != nil {
		return diagnostic{"tmux", "fail", "tmux was not found", "Install tmux and ensure it is on PATH."}
	}
	version, err := exec.Command(path, "-V").CombinedOutput()
	if err != nil {
		return diagnostic{"tmux", "fail", strings.TrimSpace(string(version)), "Repair the tmux installation."}
	}
	socket := fmt.Sprintf("agent-symphony-doctor-%d-%d", os.Getpid(), time.Now().UnixNano())
	if out, err := exec.Command(path, "-L", socket, "new-session", "-d", "-s", "doctor").CombinedOutput(); err != nil {
		return diagnostic{"tmux", "fail", strings.TrimSpace(string(out)), "Ensure tmux can create detached sessions for this user."}
	}
	defer exec.Command(path, "-L", socket, "kill-server").Run()
	return diagnostic{"tmux", "pass", strings.TrimSpace(string(version)) + "; detached session lifecycle works", ""}
}

func githubDiagnostics(repository string) []diagnostic {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, githubAPI+"/repos/"+repository, nil)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	if token := firstNonempty(os.Getenv("GITHUB_TOKEN"), os.Getenv("GH_TOKEN")); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := githubClient.Do(req)
	if err != nil {
		return []diagnostic{{"GitHub connectivity", "fail", err.Error(), "Check DNS, proxy, and access to api.github.com."}}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return []diagnostic{{"GitHub connectivity", "fail", resp.Status, "Check repository name and provide GITHUB_TOKEN or GH_TOKEN for private repositories."}}
	}
	var body struct {
		Permissions map[string]bool `json:"permissions"`
	}
	_ = json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&body)
	if len(body.Permissions) == 0 {
		return []diagnostic{
			{"GitHub connectivity", "pass", "connected to " + repository, ""},
			{"GitHub permissions", "warn", "effective repository permissions are unavailable without authenticated metadata", "Set GITHUB_TOKEN or GH_TOKEN for this read-only probe; downstream App setup must still verify feature-specific permissions."},
		}
	}
	var granted []string
	for _, name := range []string{"admin", "maintain", "push", "triage", "pull"} {
		if body.Permissions[name] {
			granted = append(granted, name)
		}
	}
	return []diagnostic{
		{"GitHub connectivity", "pass", "connected to " + repository, ""},
		{"GitHub permissions", "warn", "effective repository access: " + strings.Join(granted, ", "), "Downstream App setup must verify issue, pull-request, checks, webhook, rules, and installation permissions."},
	}
}

func repositoryFromGit() (string, error) {
	out, err := exec.Command("git", "config", "--get", "remote.origin.url").Output()
	if err != nil {
		return "", err
	}
	return parseGitHubRemote(strings.TrimSpace(string(out)))
}

func parseGitHubRemote(remote string) (string, error) {
	if strings.HasPrefix(remote, "git@github.com:") {
		remote = strings.TrimPrefix(remote, "git@github.com:")
	} else {
		u, err := url.Parse(remote)
		if err != nil || u.Hostname() != "github.com" {
			return "", fmt.Errorf("origin is not a GitHub URL")
		}
		remote = strings.TrimPrefix(u.Path, "/")
	}
	remote = strings.TrimSuffix(remote, ".git")
	if len(strings.Split(remote, "/")) != 2 {
		return "", fmt.Errorf("origin must identify one owner/repository")
	}
	if err := config.Default(remote).Validate(); err != nil {
		return "", fmt.Errorf("invalid GitHub origin: %w", err)
	}
	return remote, nil
}

func isWSL() bool {
	b, _ := os.ReadFile("/proc/sys/kernel/osrelease")
	return strings.Contains(strings.ToLower(string(b)), "microsoft")
}

func mountedFilesystem(path, mountsPath string) (string, error) {
	b, err := os.ReadFile(mountsPath)
	if err != nil {
		return "", err
	}
	path, err = filepath.EvalSymlinks(path)
	if err != nil {
		return "", err
	}
	bestMount, bestType := "", ""
	for _, line := range strings.Split(string(b), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 3 {
			continue
		}
		mount := decodeMountField(fields[1])
		if resolved, err := filepath.EvalSymlinks(mount); err == nil {
			mount = resolved
		}
		if (path == mount || strings.HasPrefix(path, strings.TrimSuffix(mount, "/")+"/")) && len(mount) > len(bestMount) {
			bestMount, bestType = mount, fields[2]
		}
	}
	if bestMount == "" {
		return "", fmt.Errorf("no /proc/mounts entry contains Git root %q", path)
	}
	return bestType, nil
}

func decodeMountField(value string) string {
	return strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`).Replace(value)
}

func firstNonempty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func success(w io.Writer, jsonOutput bool, command string, data any, human string) int {
	if jsonOutput {
		return writeJSON(w, envelope{Version: outputVersion, Command: command, OK: true, Data: data})
	}
	fmt.Fprintln(w, human)
	return 0
}

func fail(w io.Writer, jsonOutput bool, command, message string) int {
	if jsonOutput {
		writeJSON(w, envelope{Version: outputVersion, Command: command, OK: false, Error: message})
	} else {
		fmt.Fprintln(w, "error: "+message)
	}
	return 1
}

func misuse(w io.Writer, jsonOutput bool, command, message string) int {
	if jsonOutput {
		writeJSON(w, envelope{Version: outputVersion, Command: command, OK: false, Error: message})
	} else {
		fmt.Fprintln(w, "error: "+message)
	}
	return 2
}

func hasJSONFlag(args []string) bool {
	for _, arg := range args {
		if arg == "--json" {
			return true
		}
		if strings.HasPrefix(arg, "--json=") {
			value, err := strconv.ParseBool(strings.TrimPrefix(arg, "--json="))
			if err == nil && value {
				return true
			}
		}
	}
	return false
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func writeJSON(w io.Writer, value envelope) int {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(value); err != nil {
		return 1
	}
	return 0
}

func usage(w io.Writer) {
	fmt.Fprintln(w, `usage: agent-symphony <command> [options]

commands:
  init          create .agent-symphony.yaml with safe defaults
  validate      validate configuration
  config view   print validated configuration
  doctor        diagnose local prerequisites and GitHub access
  diagnostics   alias for doctor
  pr-governance reconcile pull-request governance once

options:
  --config path use another configuration file
  --json        emit a versioned JSON envelope`)
}
