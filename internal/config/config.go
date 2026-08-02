package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

const DefaultPath = ".agent-symphony.yaml"

type Config struct {
	Version            int                `json:"version"`
	Repository         string             `json:"repository"`
	Labels             Labels             `json:"labels"`
	Dependencies       Dependencies       `json:"dependencies"`
	CompletionPolicies CompletionPolicies `json:"completion_policies"`
	Concurrency        int                `json:"concurrency"`
	WorktreeRoot       string             `json:"worktree_root"`
	DocsPaths          []string           `json:"docs_paths"`
	Commands           Commands           `json:"commands"`
	Status             Status             `json:"status"`
}

type Labels struct {
	Ready      string `json:"ready"`
	PriorityP1 string `json:"priority_p1"`
	PriorityP2 string `json:"priority_p2"`
	PriorityP3 string `json:"priority_p3"`
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
	Implementation []string `json:"implementation"`
	Reviewer       []string `json:"reviewer"`
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
		Concurrency:  1,
		WorktreeRoot: ".worktrees",
		DocsPaths:    []string{"README.md", "docs"},
		Commands:     Commands{Implementation: []string{"codex", "exec"}, Reviewer: []string{"codex", "review"}},
		Status:       Status{Format: "human", Color: "auto"},
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
	if err := dec.Decode(&c); err != nil {
		return Config{}, fmt.Errorf("parse %s: %w", path, err)
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return Config{}, fmt.Errorf("parse %s: multiple JSON values", path)
	}
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
	labels := []string{c.Labels.Ready, c.Labels.PriorityP1, c.Labels.PriorityP2, c.Labels.PriorityP3, c.CompletionPolicies.HumanReview, c.CompletionPolicies.AutonomousMerge}
	seen := map[string]bool{}
	for _, label := range labels {
		if strings.TrimSpace(label) == "" {
			problems = append(problems, "labels must not be empty")
			break
		}
		if seen[label] {
			problems = append(problems, fmt.Sprintf("label %q is used more than once", label))
		}
		seen[label] = true
	}
	if c.Dependencies.Section == "" {
		problems = append(problems, "dependencies.section must not be empty")
	}
	if c.CompletionPolicies.Default != "human-review" && c.CompletionPolicies.Default != "autonomous-merge" {
		problems = append(problems, "completion_policies.default must be human-review or autonomous-merge")
	}
	if c.Concurrency < 1 {
		problems = append(problems, "concurrency must be at least 1")
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
