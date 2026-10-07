package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
)

func main() {
	if err := runLivePilotCodex(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runLivePilotCodex() error {
	if result := os.Getenv("AGENT_SYMPHONY_REVIEW_RESULT"); result != "" {
		return os.WriteFile(result, []byte(`{"type":"agent-symphony-review-v1","status":"clean","findings":[]}`), 0o600)
	}

	runID := os.Getenv("AGENT_SYMPHONY_LIVE_RUN_ID")
	result := os.Getenv("AGENT_SYMPHONY_IMPLEMENTATION_RESULT")
	if runID == "" || result == "" {
		return errors.New("live pilot worker environment is incomplete")
	}
	if err := os.WriteFile("LIVE_PILOT.md", []byte(fmt.Sprintf("isolated live pilot %s\n", runID)), 0o600); err != nil {
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
