package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestLoadAndValidate(t *testing.T) {
	c := Default("owner/repo")
	if c.ReconciliationIntervalSeconds != 60 {
		t.Fatalf("default reconciliation interval = %d", c.ReconciliationIntervalSeconds)
	}
	if !slices.Equal(c.Commands.Implementation, []string{"codex", "exec", "--dangerously-bypass-approvals-and-sandbox", "-"}) || !slices.Equal(c.Commands.Reviewer, []string{"codex", "exec", "--dangerously-bypass-approvals-and-sandbox", "-"}) {
		t.Fatalf("unexpected default commands: %#v", c.Commands)
	}
	wantOrchestrator := []string{"codex", "-c", `projects={"{orchestrator_workspace}"={trust_level="trusted"}}`, "--sandbox", "danger-full-access", "--ask-for-approval", "never", "--no-alt-screen"}
	if !slices.Equal(c.Commands.Orchestrator, wantOrchestrator) {
		t.Fatalf("unexpected default orchestrator: %#v", c.Commands.Orchestrator)
	}
	wantAudit := []string{"codex", "exec", "-c", `projects={"{orchestrator_workspace}"={trust_level="trusted"}}`, "-c", `model_reasoning_effort="medium"`, "--sandbox", "danger-full-access", "--skip-git-repo-check", "--ephemeral", "--output-last-message", "{orchestrator_result}", "-"}
	if !slices.Equal(c.Commands.OrchestratorAudit, wantAudit) {
		t.Fatalf("unexpected default orchestrator audit: %#v", c.Commands.OrchestratorAudit)
	}
	c.Commands.Implementation = []string{"custom-agent", "--flag"}
	path := filepath.Join(t.TempDir(), DefaultPath)
	if err := Write(path, c); err != nil {
		t.Fatal(err)
	}
	c, err := load(path, filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	if c.Repository != "owner/repo" || c.Concurrency != 1 || c.ReconciliationIntervalSeconds != 60 || c.CompletionPolicies.Default != "human-review" || !slices.Equal(c.Commands.Implementation, []string{"custom-agent", "--flag"}) {
		t.Fatalf("unexpected defaults: %#v", c)
	}
}

func TestReconciliationIntervalConfiguration(t *testing.T) {
	c := Default("owner/repo")
	c.ReconciliationIntervalSeconds = 20
	path := filepath.Join(t.TempDir(), DefaultPath)
	if err := Write(path, c); err != nil {
		t.Fatal(err)
	}
	loaded, err := load(path, filepath.Dir(path))
	if err != nil || loaded.ReconciliationIntervalSeconds != 20 {
		t.Fatalf("configured reconciliation interval = %d, err=%v", loaded.ReconciliationIntervalSeconds, err)
	}

	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	legacy := strings.Replace(string(b), "  \"reconciliation_interval_seconds\": 20,\n", "", 1)
	if err := os.WriteFile(path, []byte(legacy), 0o600); err != nil {
		t.Fatal(err)
	}
	loaded, err = load(path, filepath.Dir(path))
	if err != nil || loaded.ReconciliationIntervalSeconds != 60 {
		t.Fatalf("legacy reconciliation interval = %d, err=%v", loaded.ReconciliationIntervalSeconds, err)
	}
	null := strings.Replace(string(b), `"reconciliation_interval_seconds": 20`, `"reconciliation_interval_seconds": null`, 1)
	if err := os.WriteFile(path, []byte(null), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := load(path, filepath.Dir(path)); err == nil || !strings.Contains(err.Error(), "reconciliation_interval_seconds must be between 1 and 60") {
		t.Fatalf("null reconciliation interval error = %v", err)
	}

	for _, value := range []int{0, 61} {
		c.ReconciliationIntervalSeconds = value
		if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "reconciliation_interval_seconds must be between 1 and 60") {
			t.Fatalf("interval %d error = %v", value, err)
		}
	}
}

func TestOptionalIssueFilterLabel(t *testing.T) {
	c := Default("owner/repo")
	if c.Labels.IssueFilter != "" {
		t.Fatalf("default issue filter = %q", c.Labels.IssueFilter)
	}
	path := filepath.Join(t.TempDir(), DefaultPath)
	if err := Write(path, c); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), `"issue_filter"`) {
		t.Fatalf("default configuration contains an issue filter: %s", b)
	}

	empty := strings.Replace(string(b), `"ready": "agent-ready"`, `"issue_filter": "",`+"\n"+`    "ready": "agent-ready"`, 1)
	if err := os.WriteFile(path, []byte(empty), 0o600); err != nil {
		t.Fatal(err)
	}
	if loaded, err := load(path, filepath.Dir(path)); err != nil || loaded.Labels.IssueFilter != "" {
		t.Fatalf("empty issue filter = %q, err=%v", loaded.Labels.IssueFilter, err)
	}

	c.Labels.IssueFilter = "agent-work"
	if err := Write(path, c); err != nil {
		t.Fatal(err)
	}
	if loaded, err := load(path, filepath.Dir(path)); err != nil || loaded.Labels.IssueFilter != "agent-work" {
		t.Fatalf("configured issue filter = %q, err=%v", loaded.Labels.IssueFilter, err)
	}

	c.Labels.IssueFilter = c.Labels.Ready
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "used more than once") {
		t.Fatalf("duplicate issue filter error = %v", err)
	}
	c.Labels.IssueFilter = " "
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "labels must not be empty") {
		t.Fatalf("blank issue filter error = %v", err)
	}
}

func TestLoadNormalizesTheLegacyDefaultCodexStdinCommand(t *testing.T) {
	c := Default("owner/repo")
	c.Commands.Implementation = c.Commands.Implementation[:3]
	path := filepath.Join(t.TempDir(), DefaultPath)
	if err := Write(path, c); err != nil {
		t.Fatal(err)
	}
	loaded, err := load(path, filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(loaded.Commands.Implementation, Default("owner/repo").Commands.Implementation) {
		t.Fatalf("legacy implementation command was not normalized: %q", loaded.Commands.Implementation)
	}
}

func TestDefaultAndConfiguredEnvironmentExcludeHome(t *testing.T) {
	c := Default("owner/repo")
	for _, name := range c.Commands.Environment {
		if name == "HOME" {
			t.Fatal("default allowlist contains HOME")
		}
	}
	c.Commands.Environment = append(c.Commands.Environment, "HOME")
	if err := c.Validate(); err == nil {
		t.Fatal("configured HOME allowlist accepted")
	}
}

func TestLoadRejectsUnknownKeysAndSecrets(t *testing.T) {
	b, err := json.MarshalIndent(Default("owner/repo"), "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	valid := string(b)
	tests := []struct {
		name, content, want string
	}{
		{"desktop client", strings.Replace(valid, `"version": 1`, `"desktop_client": true, "version": 1`, 1), "unknown field"},
		{"secret", strings.Replace(valid, `"version": 1`, `"github_token": "canary", "version": 1`, 1), "secret-shaped"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), DefaultPath)
			if err := os.WriteFile(path, []byte(test.content), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := load(path, filepath.Dir(path))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("got %v, want error containing %q", err, test.want)
			}
		})
	}
}

func TestValidateRejectsUnsafePathsAndPolicy(t *testing.T) {
	tests := []struct {
		name string
		edit func(*Config)
		want string
	}{
		{"traversal", func(c *Config) { c.WorktreeRoot = "../outside" }, "repository-relative"},
		{"git metadata", func(c *Config) { c.DocsPaths = []string{".git/config"} }, "protected"},
		{"bad policy", func(c *Config) { c.CompletionPolicies.Default = "force" }, "must be human-review"},
		{"autonomous default", func(c *Config) { c.CompletionPolicies.Default = "autonomous-merge" }, "must be human-review"},
		{"unsafe command", func(c *Config) { c.Commands.Implementation = []string{"codex\nrm"} }, "unsafe argument"},
		{"credential flag", func(c *Config) { c.Commands.Implementation = []string{"codex", "--token=canary"} }, "credential-shaped"},
		{"credential assignment", func(c *Config) { c.Commands.Reviewer = []string{"codex", "API_KEY=canary"} }, "credential-shaped"},
		{"empty orchestrator", func(c *Config) { c.Commands.Orchestrator = []string{} }, "must contain an executable"},
		{"unsafe orchestrator", func(c *Config) { c.Commands.Orchestrator = []string{"codex", "bad\narg"} }, "unsafe argument"},
		{"credential orchestrator", func(c *Config) { c.Commands.Orchestrator = []string{"codex", "--password=canary"} }, "credential-shaped"},
		{"unsafe orchestrator audit", func(c *Config) { c.Commands.OrchestratorAudit = []string{"codex", "bad\narg"} }, "unsafe argument"},
		{"credential environment", func(c *Config) { c.Commands.Environment = []string{"GITHUB_TOKEN"} }, "forbidden credential"},
		{"invalid environment", func(c *Config) { c.Commands.Environment = []string{"bad-name"} }, "invalid variable"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := Default("owner/repo")
			test.edit(&c)
			err := c.Validate()
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("got %v, want error containing %q", err, test.want)
			}
		})
	}
}

func TestLoadRejectsSymlinkEscape(t *testing.T) {
	root := t.TempDir()
	if err := os.Symlink(t.TempDir(), filepath.Join(root, "docs-link")); err != nil {
		t.Fatal(err)
	}
	c := Default("owner/repo")
	c.DocsPaths = []string{"docs-link"}
	path := filepath.Join(root, DefaultPath)
	if err := Write(path, c); err != nil {
		t.Fatal(err)
	}
	_, err := load(path, root)
	if err == nil || !strings.Contains(err.Error(), "symbolic link") {
		t.Fatalf("got %v, want symlink escape error", err)
	}
}

func TestLoadRejectsDuplicateKeysRecursively(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, DefaultPath)
	content := `{"version":1,"repository":"owner/repo","labels":{"ready":"one","ready":"two"}}`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := load(path, root)
	if err == nil || !strings.Contains(err.Error(), `duplicate key "ready"`) {
		t.Fatalf("got %v, want recursive duplicate-key error", err)
	}
}

func TestLoadAnchorsPathsAtGitRoot(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "nested"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(t.TempDir(), filepath.Join(root, "docs-link")); err != nil {
		t.Fatal(err)
	}
	c := Default("owner/repo")
	c.DocsPaths = []string{"docs-link"}
	path := filepath.Join(root, "nested", DefaultPath)
	if err := Write(path, c); err != nil {
		t.Fatal(err)
	}
	_, err := load(path, root)
	if err == nil || !strings.Contains(err.Error(), "symbolic link") {
		t.Fatalf("got %v, want path anchored at Git root", err)
	}
}

func TestLoadRejectsConfigOutsideGitRoot(t *testing.T) {
	root, outside := t.TempDir(), t.TempDir()
	path := filepath.Join(outside, DefaultPath)
	if err := Write(path, Default("owner/repo")); err != nil {
		t.Fatal(err)
	}
	_, err := load(path, root)
	if err == nil || !strings.Contains(err.Error(), "inside resolved Git root") {
		t.Fatalf("got %v, want config containment error", err)
	}
}
