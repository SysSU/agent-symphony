package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
)

var livePilotRunID string
var livePilotExecutable = os.Executable

func main() {
	if err := runLivePilotCodex(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runLivePilotCodex(args []string, stdout, stderr io.Writer) error {
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
		info, err := os.Lstat(delegate)
		if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o111 == 0 || info.Mode().Perm()&0o022 != 0 {
			return errors.New("live pilot pinned sandbox Codex is invalid")
		}
		command := exec.Command(delegate, args...)
		command.Stdin, command.Stdout, command.Stderr = os.Stdin, stdout, stderr
		if err := command.Run(); err != nil {
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
