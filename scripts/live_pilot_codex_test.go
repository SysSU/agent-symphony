package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLivePilotCodexReview(t *testing.T) {
	result := filepath.Join(t.TempDir(), "review.json")
	t.Setenv("AGENT_SYMPHONY_REVIEW_RESULT", result)
	t.Setenv("AGENT_SYMPHONY_IMPLEMENTATION_RESULT", "")
	if err := runLivePilotCodex(); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(result)
	if err != nil || string(body) != `{"type":"agent-symphony-review-v1","status":"clean","findings":[]}` {
		t.Fatalf("review result = %q, err = %v", body, err)
	}
}

func TestLivePilotCodexImplementation(t *testing.T) {
	root := t.TempDir()
	bin := filepath.Join(root, "bin")
	if err := os.Mkdir(bin, 0o700); err != nil {
		t.Fatal(err)
	}
	log := filepath.Join(root, "git.log")
	git := "#!/bin/sh\nprintf '%s\\n' \"$*\" >>\"$LIVE_PILOT_GIT_LOG\"\n"
	if err := os.WriteFile(filepath.Join(bin, "git"), []byte(git), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Chdir(root)
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("LIVE_PILOT_GIT_LOG", log)
	t.Setenv("AGENT_SYMPHONY_REVIEW_RESULT", "")
	t.Setenv("AGENT_SYMPHONY_LIVE_RUN_ID", "test-run")
	result := filepath.Join(root, "implementation.json")
	t.Setenv("AGENT_SYMPHONY_IMPLEMENTATION_RESULT", result)

	if err := runLivePilotCodex(); err != nil {
		t.Fatal(err)
	}
	marker, err := os.ReadFile(filepath.Join(root, "LIVE_PILOT.md"))
	if err != nil || string(marker) != "isolated live pilot test-run\n" {
		t.Fatalf("marker = %q, err = %v", marker, err)
	}
	commands, err := os.ReadFile(log)
	if err != nil || string(commands) != "add LIVE_PILOT.md\ncommit -qm test: isolated live pilot\n" {
		t.Fatalf("git commands = %q, err = %v", commands, err)
	}
	body, err := os.ReadFile(result)
	if err != nil || !strings.Contains(string(body), `"type":"agent-symphony-result-v1"`) {
		t.Fatalf("implementation result = %q, err = %v", body, err)
	}
}
