package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestResolveLivePilotCodexNPMWrapper(t *testing.T) {
	platform := map[string]string{
		"darwin/amd64": "darwin-x64", "darwin/arm64": "darwin-arm64",
		"linux/amd64": "linux-x64", "linux/arm64": "linux-arm64",
	}[runtime.GOOS+"/"+runtime.GOARCH]
	if platform == "" {
		t.Skip("platform has no supported live pilot Codex package")
	}
	root := t.TempDir()
	packageRoot := filepath.Join(root, "lib", "node_modules", "@openai", "codex")
	wrapper := filepath.Join(packageRoot, "bin", "codex.js")
	native := filepath.Join(packageRoot, "node_modules", "@openai", "codex-"+platform, "vendor", "test-target", "bin", "codex")
	for _, dir := range []string{filepath.Dir(wrapper), filepath.Dir(native), filepath.Join(root, "bin")} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(wrapper, []byte("#!/usr/bin/env node\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	buildLivePilotNativeFixture(t, native)
	launcher := filepath.Join(root, "bin", "codex")
	if err := os.Symlink(wrapper, launcher); err != nil {
		t.Fatal(err)
	}
	want, err := filepath.EvalSymlinks(native)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := resolveLivePilotCodex(launcher)
	if err != nil || resolved != want {
		t.Fatalf("resolved=%q want=%q err=%v", resolved, want, err)
	}
	if resolved, err = resolveLivePilotCodex(native); err != nil || resolved != want {
		t.Fatalf("standalone resolved=%q want=%q err=%v", resolved, want, err)
	}
}

func TestResolveLivePilotCodexRejectsScript(t *testing.T) {
	path := filepath.Join(t.TempDir(), "codex")
	if err := os.WriteFile(path, []byte("#!/bin/sh\nprintf 'codex-cli 0.153.0\\n'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := resolveLivePilotCodex(path); err == nil {
		t.Fatal("version-compatible script was accepted as native Codex")
	}
}

func TestLivePilotCodexReview(t *testing.T) {
	result := filepath.Join(t.TempDir(), "review.json")
	t.Setenv("AGENT_SYMPHONY_REVIEW_RESULT", result)
	t.Setenv("AGENT_SYMPHONY_IMPLEMENTATION_RESULT", "")
	if err := runLivePilotCodex(nil, &bytes.Buffer{}, &bytes.Buffer{}); err != nil {
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
	previousRunID := livePilotRunID
	livePilotRunID = "test-run"
	t.Cleanup(func() { livePilotRunID = previousRunID })
	result := filepath.Join(root, "implementation.json")
	t.Setenv("AGENT_SYMPHONY_IMPLEMENTATION_RESULT", result)

	if err := runLivePilotCodex(nil, &bytes.Buffer{}, &bytes.Buffer{}); err != nil {
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

func TestLivePilotCodexVersionAndSandboxDelegation(t *testing.T) {
	var stdout bytes.Buffer
	if err := runLivePilotCodex([]string{"--version"}, &stdout, &bytes.Buffer{}); err != nil || stdout.String() != "codex-cli 0.153.0\n" {
		t.Fatalf("version output = %q, err = %v", stdout.String(), err)
	}

	root := t.TempDir()
	worker := filepath.Join(root, "codex")
	delegate := filepath.Join(root, "sandbox-codex")
	buildLivePilotNativeFixture(t, delegate)
	previousExecutable := livePilotExecutable
	previousDelegateExec := livePilotDelegateExec
	livePilotExecutable = func() (string, error) { return worker, nil }
	var gotPath string
	var gotArgs, gotEnv []string
	livePilotDelegateExec = func(path string, args, env []string) error {
		gotPath, gotArgs, gotEnv = path, append([]string(nil), args...), append([]string(nil), env...)
		return nil
	}
	t.Cleanup(func() {
		livePilotExecutable = previousExecutable
		livePilotDelegateExec = previousDelegateExec
	})
	if err := runLivePilotCodex([]string{"sandbox", "--", "probe", "sandbox-probe"}, &bytes.Buffer{}, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if gotPath != delegate || strings.Join(gotArgs, " ") != delegate+" sandbox -- probe sandbox-probe" || len(gotEnv) == 0 {
		t.Fatalf("sandbox delegation path=%q args=%q env=%d", gotPath, gotArgs, len(gotEnv))
	}
	if err := os.Remove(delegate); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("/bin/echo", delegate); err != nil {
		t.Fatal(err)
	}
	if err := runLivePilotCodex([]string{"sandbox", "--", "probe"}, &bytes.Buffer{}, &bytes.Buffer{}); err == nil {
		t.Fatal("symlinked sandbox delegate was accepted")
	}
}

func buildLivePilotNativeFixture(t *testing.T, path string) {
	t.Helper()
	source := filepath.Join(t.TempDir(), "main.go")
	if err := os.WriteFile(source, []byte("package main\nimport \"fmt\"\nfunc main() { fmt.Println(\"codex-cli 0.153.0\") }\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "build", "-o", path, source)
	command.Env = append(os.Environ(), "CGO_ENABLED=0")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build native Codex fixture: %v: %s", err, output)
	}
	if err := os.Chmod(path, 0o700); err != nil {
		t.Fatal(err)
	}
}
