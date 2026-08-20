package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"time"

	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

const MaxReconcileInterval = 60 * time.Second

// AttemptFact is the authoritative GitHub projection for one execution attempt.
// Local manifests may explain it, but never override it.
type AttemptFact struct {
	Repository   string   `json:"repository"`
	Issue        int      `json:"issue"`
	Attempt      int      `json:"attempt"`
	BaseSHA      string   `json:"base_sha"`
	HeadSHA      string   `json:"head_sha,omitempty"`
	PR           int      `json:"pr,omitempty"`
	State        string   `json:"state"`
	Priority     int      `json:"priority,omitempty"`
	Dependencies []int    `json:"dependencies,omitempty"`
	Checks       []string `json:"checks,omitempty"`
	Diagnostic   string   `json:"diagnostic,omitempty"`
}

type RecoveryStatus struct {
	Repository          string                  `json:"repository"`
	Issue               int                     `json:"issue"`
	Title               string                  `json:"title,omitempty"`
	Attempt             int                     `json:"attempt"`
	State               string                  `json:"state"`
	Branch              string                  `json:"branch,omitempty"`
	Worktree            string                  `json:"worktree,omitempty"`
	Session             string                  `json:"session,omitempty"`
	Sessions            []AttemptSession        `json:"sessions,omitempty"`
	CurrentPhase        string                  `json:"current_phase,omitempty"`
	PR                  int                     `json:"pr,omitempty"`
	HeadSHA             string                  `json:"head_sha,omitempty"`
	Priority            int                     `json:"priority,omitempty"`
	Dependencies        []int                   `json:"dependencies,omitempty"`
	ImplementationAgent string                  `json:"implementation_agent,omitempty"`
	ReviewAgent         string                  `json:"review_agent,omitempty"`
	Checks              []string                `json:"checks,omitempty"`
	Blockers            []string                `json:"blockers,omitempty"`
	Diagnostic          string                  `json:"diagnostic,omitempty"`
	Action              string                  `json:"next_action,omitempty"`
	Retryable           bool                    `json:"retryable,omitempty"`
	OperatorMessages    []OperatorMessageStatus `json:"operator_messages,omitempty"`
}

type AttemptSession struct {
	Role      string    `json:"role"`
	Name      string    `json:"name"`
	State     string    `json:"state"`
	Current   bool      `json:"current,omitempty"`
	CreatedAt time.Time `json:"created_at,omitzero"`
	UpdatedAt time.Time `json:"updated_at,omitzero"`
}

type OperatorMessageStatus struct {
	ID         string    `json:"id"`
	State      string    `json:"state"`
	UpdatedAt  time.Time `json:"updated_at,omitempty"`
	Diagnostic string    `json:"diagnostic,omitempty"`
}

type RuntimeCheck func(context.Context, agentruntime.Manifest, AttemptFact) error

// MatchesPublishedAttempt joins a completed local attempt to its exact durable
// pull request identity. Published work no longer depends on live worker state.
func MatchesPublishedAttempt(manifest agentruntime.Manifest, fact AttemptFact) bool {
	return manifest.State == "completed" && fact.PR > 0 &&
		(fact.State == "active" || fact.State == "review-ready" || fact.State == "completed") &&
		manifest.Repository == fact.Repository && manifest.Issue == fact.Issue && manifest.Attempt == fact.Attempt &&
		manifest.BaseSHA == fact.BaseSHA && manifest.ReviewHead != "" && manifest.ReviewHead == fact.HeadSHA
}

// ExactRuntimeCheck refuses to adopt a manifest unless the named directory,
// checked-out branch/HEAD, and exact tmux session all still agree.
func ExactRuntimeCheck(ctx context.Context, manifest agentruntime.Manifest, fact AttemptFact) error {
	info, err := os.Lstat(manifest.Worktree)
	if err != nil {
		return agentruntime.ErrWorktreeMissing
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return agentruntime.ErrWorktreeUnsafe
	}
	abs, err := filepath.Abs(manifest.Worktree)
	if err != nil || abs != filepath.Clean(manifest.Worktree) {
		return agentruntime.ErrWorktreeNonCanonical
	}
	run := func(name string, args ...string) (string, error) {
		out, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
		return strings.TrimSpace(string(out)), err
	}
	branch, err := run("git", "-C", manifest.Worktree, "branch", "--show-current")
	if err != nil || branch != manifest.Branch {
		return errors.New("worktree branch does not match manifest")
	}
	head, err := run("git", "-C", manifest.Worktree, "rev-parse", "HEAD")
	if err != nil {
		return errors.New("worktree HEAD is unreadable")
	}
	if fact.HeadSHA != "" && !strings.EqualFold(head, fact.HeadSHA) {
		return errors.New("worktree HEAD does not match GitHub")
	}
	if fact.HeadSHA == "" && !strings.EqualFold(head, fact.BaseSHA) {
		if _, err := run("git", "-C", manifest.Worktree, "merge-base", "--is-ancestor", fact.BaseSHA, head); err != nil {
			return errors.New("worktree HEAD is not descended from the approved base")
		}
	}
	if _, err := run("tmux", "has-session", "-t", "="+manifest.Session); err != nil {
		return errors.New("exact tmux session is not live")
	}
	return nil
}

// Recover builds status from fresh GitHub facts first, then joins bounded local
// manifests. It is deliberately effect-free: dispatch remains gated on a fresh
// eligibility read by the caller.
func Recover(facts []AttemptFact, local []agentruntime.Manifest) []RecoveryStatus {
	return RecoverChecked(context.Background(), facts, local, nil)
}

func RecoverChecked(ctx context.Context, facts []AttemptFact, local []agentruntime.Manifest, check RuntimeCheck) []RecoveryStatus {
	result := make([]RecoveryStatus, 0, len(facts)+len(local))
	used := make([]bool, len(local))
	seen := map[string]AttemptFact{}
	conflicting := map[string]bool{}
	issueFacts := map[string][]AttemptFact{}
	for _, fact := range facts {
		key := attemptKey(fact.Repository, fact.Issue, fact.Attempt)
		if old, ok := seen[key]; ok && !reflect.DeepEqual(old, fact) {
			conflicting[key] = true
		}
		seen[key] = fact
		issueFacts[fmt.Sprintf("%s#%d", fact.Repository, fact.Issue)] = append(issueFacts[fmt.Sprintf("%s#%d", fact.Repository, fact.Issue)], fact)
	}
	unique := make([]AttemptFact, 0, len(seen))
	for _, fact := range seen {
		unique = append(unique, fact)
	}
	for _, fact := range unique {
		key := attemptKey(fact.Repository, fact.Issue, fact.Attempt)
		if conflicting[key] {
			result = append(result, RecoveryStatus{Repository: fact.Repository, Issue: fact.Issue, Attempt: fact.Attempt, State: "conflicting", Diagnostic: "GitHub reports contradictory attempt facts", Action: "repair the coordinator-authored attempt markers, then reconcile"})
			continue
		}
		status := RecoveryStatus{Repository: fact.Repository, Issue: fact.Issue, Attempt: fact.Attempt, State: fact.State, PR: fact.PR, HeadSHA: fact.HeadSHA, Priority: fact.Priority, Dependencies: fact.Dependencies, Checks: fact.Checks, Diagnostic: fact.Diagnostic}
		issueConflict := false
		active, completed, latestAttempt := 0, 0, fact.Attempt
		retryConflict := false
		for _, other := range issueFacts[fmt.Sprintf("%s#%d", fact.Repository, fact.Issue)] {
			latestAttempt = max(latestAttempt, other.Attempt)
			if other.State == "active" || other.State == "review-ready" {
				active++
			}
			if other.State == "completed" {
				completed++
			}
			if other.Attempt != fact.Attempt && other.State != "failed" || conflicting[attemptKey(other.Repository, other.Issue, other.Attempt)] {
				retryConflict = true
			}
		}
		issueConflict = active > 1 || (active > 0 && completed > 0) || completed > 1
		if issueConflict {
			status.State, status.Blockers, status.Diagnostic, status.Action, status.Retryable = "blocked", []string{"conflicting authoritative attempts"}, "GitHub reports duplicate active/completed attempts for this issue", "repair coordinator-authored markers; dispatch is suppressed", false
		}
		for i, manifest := range local {
			if manifest.Repository != fact.Repository || manifest.Issue != fact.Issue || manifest.Attempt != fact.Attempt {
				continue
			}
			used[i] = true
			status.Branch, status.Worktree, status.Session = manifest.Branch, manifest.Worktree, manifest.Session
			status.ImplementationAgent, status.ReviewAgent = manifest.ImplementationAgent, manifest.ReviewAgent
			var checkErr error
			if (fact.State == "active" || fact.State == "review-ready") && manifest.State == "running" && check != nil {
				checkErr = check(ctx, manifest, fact)
			}
			switch {
			case fact.State == "completed":
				status.State, status.Diagnostic, status.Action, status.Retryable = "completed", "", "none; completed work must not be redispatched", false
			case issueConflict:
			case manifest.BaseSHA != fact.BaseSHA:
				status.State, status.Blockers, status.Diagnostic, status.Action, status.Retryable = "blocked", []string{"runtime base mismatch"}, "local base does not match GitHub", "preserve diagnostics and create a new traceable attempt", false
			case fact.State == "failed":
				status.Retryable = fact.Attempt == latestAttempt && !retryConflict && (manifest.State == "failed" || manifest.State == "cancelled" || manifest.State == "completed")
				if status.Retryable {
					status.Action = "recover this failed attempt to request a new attempt"
				} else {
					status.Action = "terminalize the matching local runtime before requesting a retry"
				}
				if manifest.State == "failed" {
					status.Diagnostic = manifest.Diagnostic
				}
			case manifest.State == "failed":
				status.State, status.Diagnostic, status.Action = "failed", manifest.Diagnostic, "inspect the retained log and retry with a new attempt"
			case fact.State == "active" && fact.PR == 0 && manifest.State == "completed":
				status.Action = "resume publication of the matching completed attempt"
			case MatchesPublishedAttempt(manifest, fact):
				status.Action = "monitor the matching published pull request"
			case (fact.State == "active" || fact.State == "review-ready") && manifest.State == "running" && check != nil && checkErr == nil:
				status.Action = "resume monitoring the matching attempt"
			case (fact.State == "active" || fact.State == "review-ready") && runtimeWorktreeBlocker(checkErr) != "":
				status.State, status.Blockers, status.Diagnostic, status.Action, status.Retryable = "blocked", []string{runtimeWorktreeBlocker(checkErr)}, checkErr.Error(), "repair or reconstruct the exact worktree; automatic recovery is suppressed", false
			case fact.State == "active" || fact.State == "review-ready":
				diagnostic := "local attempt is not running"
				if checkErr != nil {
					diagnostic = checkErr.Error()
				}
				status.State, status.Blockers, status.Diagnostic, status.Action, status.Retryable = "blocked", []string{"runtime liveness mismatch"}, diagnostic, "recover this attempt or restore its exact runtime resources", true
			}
			projectAttemptLifecycle(&status, manifest)
			break
		}
		if (fact.State == "active" || fact.State == "review-ready") && status.Session == "" {
			status.State, status.CurrentPhase, status.Blockers, status.Diagnostic, status.Action = "blocked", "blocked", []string{"runtime resources missing"}, "GitHub says active but local attempt resources are missing", "reconstruct the resources or create a new traceable attempt"
		}
		if status.CurrentPhase == "" {
			status.CurrentPhase = status.State
		}
		result = append(result, status)
	}
	for i, manifest := range local {
		if used[i] {
			continue
		}
		status := RecoveryStatus{Repository: manifest.Repository, Issue: manifest.Issue, Attempt: manifest.Attempt, State: "orphaned", Branch: manifest.Branch, Worktree: manifest.Worktree, Session: manifest.Session, Diagnostic: "local attempt has no authoritative GitHub marker", Action: "preserve diagnostics; do not attach or dispatch"}
		projectAttemptLifecycle(&status, manifest)
		result = append(result, status)
	}
	slices.SortFunc(result, func(a, b RecoveryStatus) int {
		if a.Repository != b.Repository {
			return compareString(a.Repository, b.Repository)
		}
		if a.Issue != b.Issue {
			return a.Issue - b.Issue
		}
		return a.Attempt - b.Attempt
	})
	return result
}

func projectAttemptLifecycle(status *RecoveryStatus, manifest agentruntime.Manifest) {
	add := func(role, name, state string, created time.Time) {
		want, err := agentruntime.AttemptSessionName(role, manifest.Repository, manifest.Issue, manifest.Attempt)
		if err == nil && name == want && state != "" {
			status.Sessions = append(status.Sessions, AttemptSession{Role: role, Name: name, State: state, CreatedAt: created, UpdatedAt: manifest.UpdatedAt})
		}
	}
	add(agentruntime.SessionRoleImplementation, manifest.Session, manifest.State, manifest.CreatedAt)
	if manifest.ReviewSession != "" {
		add(agentruntime.SessionRoleReviewer, manifest.ReviewSession, manifest.ReviewState, time.Time{})
	}

	switch status.State {
	case "completed", "failed", "cancelled", "orphaned", "conflicting":
		status.CurrentPhase = status.State
		return
	case "blocked":
		status.CurrentPhase = "blocked"
		return
	}
	switch {
	case manifest.State == "failed" || manifest.State == "cancelled":
		status.CurrentPhase = manifest.State
	case manifest.State == "preparing" || manifest.State == "running":
		if manifest.ReviewState == "findings-queued" {
			status.CurrentPhase = "findings-handoff"
		} else {
			status.CurrentPhase = "implementation"
		}
	case manifest.ReviewState == "preparing" || manifest.ReviewState == "running":
		status.CurrentPhase = "review"
	case manifest.ReviewState == "findings-queued":
		status.CurrentPhase = "findings-handoff"
	case manifest.ReviewState == "clean":
		status.CurrentPhase = "publication"
	case manifest.State == "completed":
		status.CurrentPhase = "validation"
	default:
		status.CurrentPhase = status.State
	}

	currentRole := ""
	switch status.CurrentPhase {
	case "implementation", "findings-handoff":
		currentRole = agentruntime.SessionRoleImplementation
	case "review":
		currentRole = agentruntime.SessionRoleReviewer
	}
	for i := range status.Sessions {
		status.Sessions[i].Current = status.Sessions[i].Role == currentRole
	}
	if len(status.Blockers) != 0 || (status.State != "active" && status.State != "review-ready") {
		return
	}
	switch status.CurrentPhase {
	case "implementation":
		status.Action = "monitor the implementation session"
	case "validation":
		status.Action = "validate the completed implementation result"
	case "review":
		status.Action = "monitor the independent reviewer session"
		if !slices.ContainsFunc(status.Sessions, func(session AttemptSession) bool { return session.Role == agentruntime.SessionRoleReviewer }) {
			status.Action = "restore or restart the exact reviewer session"
		}
	case "findings-handoff":
		status.Action = "deliver review findings to the implementation session"
		if manifest.ReviewHandoffAck {
			status.Action = "monitor follow-up implementation after review findings"
		}
	case "publication":
		status.Action = "publish the independently reviewed implementation"
		if status.PR > 0 {
			status.Action = "monitor the matching published pull request"
		}
	}
}

func runtimeWorktreeBlocker(err error) string {
	switch {
	case errors.Is(err, agentruntime.ErrWorktreeMissing):
		return "runtime worktree missing"
	case errors.Is(err, agentruntime.ErrWorktreeUnsafe):
		return "runtime worktree unsafe"
	case errors.Is(err, agentruntime.ErrWorktreeNonCanonical):
		return "runtime worktree noncanonical"
	default:
		return ""
	}
}

// ReconcileLoop reconciles immediately, then repeats at most every interval.
func ReconcileLoop(ctx context.Context, interval time.Duration, reconcile func(context.Context) error) error {
	if reconcile == nil {
		return errors.New("reconcile function is required")
	}
	if interval <= 0 || interval > MaxReconcileInterval {
		return fmt.Errorf("reconcile interval must be between 1ns and %s", MaxReconcileInterval)
	}
	_ = reconcile(ctx) // a bounded GitHub outage must not stop later recovery
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			_ = reconcile(ctx)
			ticker.Reset(interval)
		}
	}
}

func attemptKey(repository string, issue, attempt int) string {
	return fmt.Sprintf("%s#%d/%d", repository, issue, attempt)
}
func compareString(a, b string) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}
