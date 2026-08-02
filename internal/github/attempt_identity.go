package github

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
)

// AttemptBranch is the single branch identity used by runtime, publication, and recovery.
func AttemptBranch(repository string, issue, attempt int) (string, error) {
	parts := strings.Split(repository, "/")
	if len(parts) != 2 || issue < 1 || attempt < 1 {
		return "", errors.New("invalid attempt branch identity")
	}
	slug := strings.ToLower(strings.ReplaceAll(repository, "/", "-"))
	if len(slug) > 40 {
		slug = strings.TrimRight(slug[:40], ".-_")
	}
	sum := sha256.Sum256([]byte(repository))
	return fmt.Sprintf("agent-symphony/%s-%x/%d-%d", slug, sum[:6], issue, attempt), nil
}

func AttemptBranchFromBranch(branch string, issue, attempt int) (string, error) {
	parts := strings.Split(branch, "/")
	if len(parts) != 3 || parts[0] != "agent-symphony" {
		return "", errors.New("invalid deterministic attempt branch")
	}
	return fmt.Sprintf("agent-symphony/%s/%d-%d", parts[1], issue, attempt), nil
}
