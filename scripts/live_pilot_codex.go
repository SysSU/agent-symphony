package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"syscall"

	"github.com/SysSU/agent-symphony/internal/config"
)

var livePilotRunID string
var livePilotExecutable = os.Executable
var livePilotDelegateExec = syscall.Exec

func main() {
	if err := runLivePilotCodex(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runLivePilotCodex(args []string, stdout, stderr io.Writer) error {
	if len(args) == 2 && args[0] == "--live-pilot-resolve-native" {
		path, err := resolveLivePilotCodex(args[1])
		if err != nil {
			return err
		}
		_, err = fmt.Fprintln(stdout, path)
		return err
	}
	if len(args) == 1 && args[0] == "--version" {
		_, err := fmt.Fprintln(stdout, "codex-cli 0.153.0")
		return err
	}
	if len(args) > 0 && args[0] == "sandbox" {
		executable, err := livePilotExecutable()
		if err != nil || !filepath.IsAbs(executable) {
			return errors.New("live pilot executable path is invalid")
		}
		delegate := filepath.Join(filepath.Dir(executable), "sandbox-codex")
		if err := config.ValidateNativeCodexExecutable(delegate); err != nil {
			return errors.New("live pilot pinned sandbox Codex is invalid")
		}
		if err := livePilotDelegateExec(delegate, append([]string{delegate}, args...), os.Environ()); err != nil {
			return fmt.Errorf("delegate Codex sandbox: %w", err)
		}
		return nil
	}
	if result := os.Getenv("AGENT_SYMPHONY_REVIEW_RESULT"); result != "" {
		return os.WriteFile(result, []byte(`{"type":"agent-symphony-review-v1","status":"clean","findings":[]}`), 0o600)
	}

	result := os.Getenv("AGENT_SYMPHONY_IMPLEMENTATION_RESULT")
	if livePilotRunID == "" || result == "" {
		return errors.New("live pilot worker environment is incomplete")
	}
	if err := os.WriteFile("LIVE_PILOT.md", []byte(fmt.Sprintf("isolated live pilot %s\n", livePilotRunID)), 0o600); err != nil {
		return err
	}
	for _, args := range [][]string{{"add", "LIVE_PILOT.md"}, {"commit", "-qm", "test: isolated live pilot"}} {
		command := exec.Command("git", args...)
		command.Stdout, command.Stderr = os.Stdout, os.Stderr
		if err := command.Run(); err != nil {
			return fmt.Errorf("git %s: %w", args[0], err)
		}
	}
	return os.WriteFile(result, []byte("{\"type\":\"agent-symphony-result-v1\",\"validation\":\"live pilot commit and review lifecycle\",\"documentation\":\"temporary live pilot marker\"}\n"), 0o600)
}

func resolveLivePilotCodex(path string) (string, error) {
	if !filepath.IsAbs(path) {
		return "", errors.New("live pilot Codex path is not absolute")
	}
	canonical, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", err
	}
	if filepath.Base(canonical) == "codex.js" {
		platform := map[string]string{
			"darwin/amd64": "darwin-x64", "darwin/arm64": "darwin-arm64",
			"linux/amd64": "linux-x64", "linux/arm64": "linux-arm64",
		}[runtime.GOOS+"/"+runtime.GOARCH]
		if platform == "" {
			return "", errors.New("live pilot Codex npm wrapper has no supported native executable")
		}
		packageRoot := filepath.Dir(filepath.Dir(canonical))
		matches, globErr := filepath.Glob(filepath.Join(packageRoot, "node_modules", "@openai", "codex-"+platform, "vendor", "*", "bin", "codex"))
		if globErr != nil || len(matches) != 1 {
			return "", errors.New("live pilot Codex npm wrapper has no exact native executable")
		}
		canonical, err = filepath.EvalSymlinks(matches[0])
		if err != nil {
			return "", err
		}
	}
	if err := config.ValidateNativeCodexExecutable(canonical); err != nil {
		return "", errors.New("live pilot native Codex is invalid")
	}
	return canonical, nil
}
