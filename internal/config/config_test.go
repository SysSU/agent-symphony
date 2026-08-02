package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadAndValidate(t *testing.T) {
	path := filepath.Join(t.TempDir(), DefaultPath)
	if err := Write(path, Default("owner/repo")); err != nil {
		t.Fatal(err)
	}
	c, err := load(path, filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	if c.Repository != "owner/repo" || c.Concurrency != 1 || c.CompletionPolicies.Default != "human-review" {
		t.Fatalf("unexpected defaults: %#v", c)
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
		{"unknown", strings.Replace(valid, `"version": 1`, `"mystery": true, "version": 1`, 1), "unknown field"},
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
		{"bad policy", func(c *Config) { c.CompletionPolicies.Default = "force" }, "human-review or autonomous-merge"},
		{"unsafe command", func(c *Config) { c.Commands.Implementation = []string{"codex\nrm"} }, "unsafe argument"},
		{"credential flag", func(c *Config) { c.Commands.Implementation = []string{"codex", "--token=canary"} }, "credential-shaped"},
		{"credential assignment", func(c *Config) { c.Commands.Reviewer = []string{"codex", "API_KEY=canary"} }, "credential-shaped"},
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
