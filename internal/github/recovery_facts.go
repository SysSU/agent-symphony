package github

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"
)

const recoveryMarkerPrefix = "<!-- agent-symphony:attempt:v1\n"
const terminalMarkerPrefix = "<!-- agent-symphony:terminal:v1\n"
const activeMarkerPrefix = "<!-- agent-symphony:active-attempt:v1\n"
const recoveryPageLimit = 10
const recoveryPullsPerPage = 25

type recoveryMarkerPayload struct {
	Version int    `json:"version"`
	Issue   int    `json:"issue"`
	Attempt int    `json:"attempt"`
	Branch  string `json:"branch"`
	Head    string `json:"head"`
	PR      int    `json:"pr"`
	Outcome string `json:"outcome"`
}

type terminalMarkerPayload struct {
	Version, Issue, Attempt int
	Outcome                 string
	FailedAt                time.Time `json:"failed_at"`
	Diagnostic              string    `json:"-"`
}

type activeMarkerPayload struct {
	Version int    `json:"version"`
	Issue   int    `json:"issue"`
	Attempt int    `json:"attempt"`
	Branch  string `json:"branch"`
	BaseSHA string `json:"base_sha"`
}

func ActiveAttemptMarker(repository string, issue, attempt int, baseSHA string) (string, error) {
	branch, err := AttemptBranch(repository, issue, attempt)
	if err != nil || !regexpSHA.MatchString(baseSHA) {
		return "", errors.New("active attempt marker requires its deterministic branch and approved base")
	}
	b, _ := json.Marshal(activeMarkerPayload{Version: 1, Issue: issue, Attempt: attempt, Branch: branch, BaseSHA: baseSHA})
	return activeMarkerPrefix + string(b) + "\n-->", nil
}

func parseActiveAttemptMarker(body string) (activeMarkerPayload, error) {
	start := strings.Index(body, activeMarkerPrefix)
	if start < 0 || len(body) > 64<<10 || strings.Count(body, activeMarkerPrefix) != 1 {
		return activeMarkerPayload{}, errors.New("active attempt marker is missing or duplicated")
	}
	rest := body[start+len(activeMarkerPrefix):]
	end := strings.Index(rest, "\n-->")
	if end < 0 || strings.TrimSpace(rest[end+4:]) != "" {
		return activeMarkerPayload{}, errors.New("active attempt marker is malformed")
	}
	var marker activeMarkerPayload
	decoder := json.NewDecoder(strings.NewReader(rest[:end]))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&marker) != nil || decoder.Decode(&struct{}{}) != io.EOF || marker.Version != 1 || marker.Issue < 1 || marker.Attempt < 1 || marker.Branch == "" || !regexpSHA.MatchString(marker.BaseSHA) {
		return activeMarkerPayload{}, errors.New("active attempt marker is invalid")
	}
	return marker, nil
}

func TerminalFailureMarker(issue, attempt int, failedAt time.Time) (string, error) {
	if issue < 1 || attempt < 1 || failedAt.IsZero() {
		return "", errors.New("terminal marker requires positive issue and attempt")
	}
	b, _ := json.Marshal(terminalMarkerPayload{Version: 1, Issue: issue, Attempt: attempt, Outcome: "failed", FailedAt: failedAt.UTC()})
	return terminalMarkerPrefix + string(b) + "\n-->", nil
}

func parseTerminalMarker(body string) (terminalMarkerPayload, error) {
	start := strings.Index(body, terminalMarkerPrefix)
	if start < 0 || len(body) > 64<<10 || strings.Count(body, terminalMarkerPrefix) != 1 {
		return terminalMarkerPayload{}, errors.New("terminal marker missing")
	}
	rest := body[start+len(terminalMarkerPrefix):]
	end := strings.Index(rest, "\n-->")
	if end < 0 || strings.TrimSpace(rest[end+4:]) != "" {
		return terminalMarkerPayload{}, errors.New("terminal marker malformed")
	}
	var marker terminalMarkerPayload
	decoder := json.NewDecoder(strings.NewReader(rest[:end]))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&marker) != nil || marker.Version != 1 || marker.Issue < 1 || marker.Attempt < 1 || marker.Outcome != "failed" || marker.FailedAt.IsZero() {
		return terminalMarkerPayload{}, errors.New("terminal marker invalid")
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return terminalMarkerPayload{}, errors.New("terminal marker trailing data")
	}
	prefix := strings.TrimSpace(body[:start])
	if line := strings.SplitN(prefix, "\n", 2)[0]; strings.HasPrefix(line, "Attempt failed closed: ") {
		marker.Diagnostic = strings.TrimPrefix(line, "Attempt failed closed: ")
	}
	return marker, nil
}

func parseAttemptMarker(body string) (recoveryMarkerPayload, error) {
	start := strings.Index(body, recoveryMarkerPrefix)
	if start < 0 || len(body) > 64<<10 {
		return recoveryMarkerPayload{}, errors.New("strict v1 attempt marker is missing")
	}
	rest := body[start+len(recoveryMarkerPrefix):]
	end := strings.Index(rest, "\n-->")
	if end < 0 || strings.Contains(rest[end+4:], recoveryMarkerPrefix) {
		return recoveryMarkerPayload{}, errors.New("attempt marker is malformed or duplicated")
	}
	var marker recoveryMarkerPayload
	decoder := json.NewDecoder(strings.NewReader(rest[:end]))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&marker); err != nil || marker.Version != 1 || marker.Issue < 1 || marker.Attempt < 1 || marker.Branch == "" || !regexpSHA.MatchString(marker.Head) || marker.PR < 1 || marker.Outcome != "review" {
		return recoveryMarkerPayload{}, errors.New("invalid attempt marker schema")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return recoveryMarkerPayload{}, errors.New("attempt marker has trailing data")
	}
	return marker, nil
}

// FetchAttemptFacts reconstructs attempts from current marked PRs and their
// current issue/head/check facts. It never uses local state.
type RecoveryAttemptFact struct {
	Repository              string
	Issue, Attempt, PR      int
	BaseSHA, HeadSHA, State string
	PublicationConfirmed    bool
	Diagnostic              string
	Checks                  []string
}

type RecoveryIssueFact struct {
	Repository, Title, Body, BaseSHA, BaseBranch  string
	Issue, Attempt, Priority                      int
	CreatedAt                                     time.Time
	Dependencies                                  []int
	Paths                                         []string
	Blockers                                      []string
	Eligible, Active, Completed, Retry, Cancelled bool
	DispatchAuthorized                            bool
	RecoveryAuthorized                            bool
	RecoveryAttempt                               int
	NeedsAttention                                bool
	ActiveAttempt                                 *RecoveryAttemptFact
	TerminalAttempts                              []RecoveryAttemptFact
}

const (
	directStatusPrefix = "/agent-symphony status "
	directStatusLabel  = "needs-attention"
)

type directStatus struct {
	NeedsAttention bool
	Reason         string
	createdAt      time.Time
	commentID      int64
}

func parseDirectStatus(body string) (directStatus, bool) {
	command, reason, found := strings.Cut(strings.TrimSpace(body), ":")
	if !found || len(reason) > 1024 || strings.ContainsRune(reason, 0) {
		return directStatus{}, false
	}
	status := directStatus{Reason: strings.TrimSpace(reason)}
	if status.Reason == "" {
		return directStatus{}, false
	}
	switch command {
	case directStatusPrefix + "needs-attention":
		status.NeedsAttention = true
	case directStatusPrefix + "clear":
	default:
		return directStatus{}, false
	}
	return status, true
}

func (s *GitHubPRSource) directStatus(ctx context.Context, issue, pullRequest int) (directStatus, error) {
	var latest directStatus
	for _, number := range []int{issue, pullRequest} {
		if number == 0 {
			continue
		}
		comments, err := s.issueComments(ctx, number)
		if err != nil {
			return directStatus{}, err
		}
		for _, comment := range comments {
			status, ok := parseDirectStatus(comment.Body)
			if !ok || comment.User.ID != s.Config.ActorID || !comment.CreatedAt.Equal(comment.UpdatedAt) {
				continue
			}
			if latest.commentID == 0 || comment.CreatedAt.After(latest.createdAt) || comment.CreatedAt.Equal(latest.createdAt) && comment.ID > latest.commentID {
				status.createdAt, status.commentID = comment.CreatedAt, comment.ID
				latest = status
			}
		}
	}
	var current struct{ Labels []struct{ Name string } }
	if _, _, err := s.API.Read(ctx, fmt.Sprintf("/repos/%s/issues/%d", s.Config.Repository, issue), "", &current); err != nil {
		return directStatus{}, err
	}
	labelPresent := slices.ContainsFunc(current.Labels, func(label struct{ Name string }) bool { return label.Name == directStatusLabel })
	if latest.commentID == 0 {
		if labelPresent {
			return directStatus{NeedsAttention: true, Reason: "needs-attention label requires an explanatory status comment"}, nil
		}
		return latest, nil
	}
	if latest.NeedsAttention != labelPresent {
		if labelPresent {
			latest.Reason = "direct status clear is incomplete: needs-attention label remains"
		} else {
			latest.Reason = "direct needs-attention status is incomplete: needs-attention label is missing"
		}
		latest.NeedsAttention = true
	}
	return latest, nil
}

type markerConflicts struct {
	Any      bool
	Attempts map[int]bool
}

// FetchIssueFacts returns the authorized issue-control projection used by both
// scheduling and read-only status. Intake permits the reconciliation command
// to create a missing control snapshot; status calls remain read-only.
func FetchIssueFacts(ctx context.Context, api API, cfg PRAdapterConfig, attempts []RecoveryAttemptFact, intake bool) ([]RecoveryIssueFact, error) {
	return fetchIssueFacts(ctx, api, cfg, attempts, intake, 0)
}

// FetchOperatorIssueFacts reads only the issue that owns one operator message.
func FetchOperatorIssueFacts(ctx context.Context, api API, cfg PRAdapterConfig, attempts []RecoveryAttemptFact, issue int) ([]RecoveryIssueFact, error) {
	if issue < 1 {
		return nil, errors.New("operator issue lookup requires a positive issue")
	}
	return fetchIssueFacts(ctx, api, cfg, attempts, false, issue)
}

func fetchIssueFacts(ctx context.Context, api API, cfg PRAdapterConfig, attempts []RecoveryAttemptFact, intake bool, targetIssue int) ([]RecoveryIssueFact, error) {
	var repository struct {
		DefaultBranch string `json:"default_branch"`
	}
	if _, _, err := api.Read(ctx, "/repos/"+cfg.Repository, "", &repository); err != nil {
		return nil, err
	}
	var branch struct {
		Commit struct{ SHA string } `json:"commit"`
	}
	if repository.DefaultBranch == "" {
		return nil, errors.New("repository default branch is missing")
	}
	if _, _, err := api.Read(ctx, fmt.Sprintf("/repos/%s/branches/%s", cfg.Repository, repository.DefaultBranch), "", &branch); err != nil {
		return nil, err
	}
	active, completed, next := map[int]bool{}, map[int]bool{}, map[int]int{}
	for _, attempt := range attempts {
		if attempt.Attempt >= next[attempt.Issue] {
			next[attempt.Issue] = attempt.Attempt + 1
		}
		active[attempt.Issue] = active[attempt.Issue] || attempt.State == "active" || attempt.State == "review-ready"
		completed[attempt.Issue] = completed[attempt.Issue] || attempt.State == "completed"
	}
	source := GitHubPRSource{API: api, Config: cfg}
	var result []RecoveryIssueFact
	for page := 1; page <= recoveryPageLimit; page++ {
		var issues []struct {
			Number             int
			Title, Body, State string
			CreatedAt          time.Time `json:"created_at"`
			PullRequest        any       `json:"pull_request"`
		}
		if targetIssue > 0 {
			var issue struct {
				Number             int
				Title, Body, State string
				CreatedAt          time.Time `json:"created_at"`
				PullRequest        any       `json:"pull_request"`
			}
			if _, _, err := api.Read(ctx, fmt.Sprintf("/repos/%s/issues/%d", cfg.Repository, targetIssue), "", &issue); err != nil {
				return nil, err
			}
			if issue.State != "open" || issue.PullRequest != nil {
				return result, nil
			}
			issues = append(issues, issue)
		} else if _, _, err := api.Read(ctx, fmt.Sprintf("/repos/%s/issues?state=open&per_page=100&page=%d", cfg.Repository, page), "", &issues); err != nil {
			return nil, err
		}
		for _, issue := range issues {
			if issue.PullRequest != nil {
				continue
			}
			bindings, bindingConflicts, err := fetchActiveAttempts(ctx, api, cfg, issue.Number)
			if err != nil {
				return nil, err
			}
			terminals, terminalConflicts, err := fetchTerminalFailures(ctx, api, cfg, issue.Number)
			if err != nil {
				return nil, err
			}
			var terminal terminalMarkerPayload
			if len(terminals) > 0 {
				terminal = terminals[len(terminals)-1]
			}
			terminalByAttempt := make(map[int]terminalMarkerPayload, len(terminals))
			for _, marker := range terminals {
				terminalByAttempt[marker.Attempt] = marker
			}
			var binding activeMarkerPayload
			var terminalAttempts []RecoveryAttemptFact
			for _, candidate := range bindings {
				if failed, ok := terminalByAttempt[candidate.Attempt]; ok && !bindingConflicts.Attempts[candidate.Attempt] && !terminalConflicts.Attempts[candidate.Attempt] {
					terminalAttempts = append(terminalAttempts, RecoveryAttemptFact{Repository: cfg.Repository, Issue: issue.Number, Attempt: candidate.Attempt, BaseSHA: candidate.BaseSHA, State: "failed", Diagnostic: failed.Diagnostic})
					continue
				}
				bindingFinal := slices.ContainsFunc(attempts, func(attempt RecoveryAttemptFact) bool {
					return attempt.Issue == issue.Number && attempt.Attempt == candidate.Attempt
				})
				if bindingFinal {
					continue
				}
				if binding.Attempt != 0 {
					bindingConflicts.Any = true
					continue
				}
				binding = candidate
			}
			if binding.Attempt >= next[issue.Number] {
				next[issue.Number] = binding.Attempt + 1
			}
			for _, marker := range terminals {
				if marker.Attempt >= next[issue.Number] {
					next[issue.Number] = marker.Attempt + 1
				}
			}
			pullRequest := 0
			for _, attempt := range attempts {
				if attempt.Issue == issue.Number && attempt.Attempt >= next[issue.Number]-1 {
					pullRequest = attempt.PR
				}
			}
			status, err := source.directStatus(ctx, issue.Number, pullRequest)
			if err != nil {
				return nil, fmt.Errorf("read direct status for issue #%d: %w", issue.Number, err)
			}
			controls, _, retry, err := source.authorizedControlsWithIntake(ctx, issue.Number, intake)
			if err != nil {
				attempt := max(1, next[issue.Number])
				var activeAttempt *RecoveryAttemptFact
				if binding.Attempt > 0 {
					attempt = binding.Attempt
					activeAttempt = &RecoveryAttemptFact{Repository: cfg.Repository, Issue: issue.Number, Attempt: binding.Attempt, BaseSHA: binding.BaseSHA, State: "active"}
				}
				blockers := []string{err.Error()}
				if status.NeedsAttention {
					blockers = append(blockers, "needs attention: "+status.Reason)
				}
				result = append(result, RecoveryIssueFact{Repository: cfg.Repository, Issue: issue.Number, Attempt: attempt, Title: issue.Title, Body: issue.Body, BaseSHA: branch.Commit.SHA, BaseBranch: repository.DefaultBranch, CreatedAt: issue.CreatedAt, Blockers: blockers, Active: active[issue.Number] || binding.Attempt > 0 || bindingConflicts.Any, Completed: completed[issue.Number], NeedsAttention: status.NeedsAttention, ActiveAttempt: activeAttempt, TerminalAttempts: terminalAttempts})
				continue
			}
			blockers := []string{}
			if status.NeedsAttention {
				blockers = append(blockers, "needs attention: "+status.Reason)
			}
			if bindingConflicts.Any {
				blockers = append(blockers, "active attempt marker is foreign, malformed, or contradictory")
			}
			if terminalConflicts.Any {
				blockers = append(blockers, "terminal attempt marker is contradictory")
			}
			for _, dependency := range controls.Dependencies {
				if targetIssue > 0 && !completed[dependency] {
					var current struct{ State string }
					if _, _, err := api.Read(ctx, fmt.Sprintf("/repos/%s/issues/%d", cfg.Repository, dependency), "", &current); err != nil {
						return nil, err
					}
					completed[dependency] = current.State == "closed"
				}
				if !completed[dependency] {
					blockers = append(blockers, fmt.Sprintf("dependency #%d is incomplete", dependency))
				}
			}
			if terminal.Attempt > 0 && !retryAuthorizesFailure(controls, retry, terminal) {
				blockers = append(blockers, fmt.Sprintf("attempt %d has a coordinator-authored terminal failure requiring a later authorized retry", terminal.Attempt))
			}
			recoveryBlockers := len(blockers)
			if terminal.Attempt > 0 && !retryAuthorizesFailure(controls, retry, terminal) {
				recoveryBlockers-- // Recover exists to resolve this one expected blocker.
			}
			filterMatches := cfg.IssueFilterLabel == "" || controls.IssueFilter
			authorized := controls.Ready && filterMatches && !controls.Closed && !controls.Cancelled && len(blockers) == 0
			bound := binding.Attempt > 0
			attempt := max(1, next[issue.Number])
			var activeAttempt *RecoveryAttemptFact
			if bound {
				attempt = binding.Attempt
				activeAttempt = &RecoveryAttemptFact{Repository: cfg.Repository, Issue: issue.Number, Attempt: binding.Attempt, BaseSHA: binding.BaseSHA, State: "active"}
			}
			isActive := active[issue.Number] || bound || bindingConflicts.Any
			eligible := authorized && !isActive && !completed[issue.Number]
			recoveryAuthorized := controls.Ready && filterMatches && !controls.Closed && !controls.Cancelled && recoveryBlockers == 0 && !completed[issue.Number]
			recoveryAttempt := 0
			if recoveryAuthorized && !isActive && slices.ContainsFunc(terminalAttempts, func(fact RecoveryAttemptFact) bool { return fact.Attempt == terminal.Attempt }) {
				recoveryAttempt = terminal.Attempt
			}
			result = append(result, RecoveryIssueFact{Repository: cfg.Repository, Issue: issue.Number, Attempt: attempt, Title: issue.Title, Body: issue.Body, BaseSHA: branch.Commit.SHA, BaseBranch: repository.DefaultBranch, CreatedAt: issue.CreatedAt, Priority: controls.Priority, Dependencies: controls.Dependencies, Paths: IssuePaths(issue.Body), Blockers: blockers, Eligible: eligible, Active: isActive, Completed: completed[issue.Number], Retry: controls.Retry, Cancelled: controls.Cancelled, DispatchAuthorized: authorized, RecoveryAuthorized: recoveryAuthorized, RecoveryAttempt: recoveryAttempt, NeedsAttention: status.NeedsAttention, ActiveAttempt: activeAttempt, TerminalAttempts: terminalAttempts})
		}
		if targetIssue > 0 || len(issues) < 100 {
			return result, nil
		}
	}
	return nil, errors.New("open issues exceed bounded recovery limit")
}

func fetchActiveAttempts(ctx context.Context, api API, cfg PRAdapterConfig, issue int) ([]activeMarkerPayload, markerConflicts, error) {
	found := map[int]activeMarkerPayload{}
	conflicts := markerConflicts{Attempts: map[int]bool{}}
	for page := 1; page <= recoveryPageLimit; page++ {
		var comments []struct {
			Body string           `json:"body"`
			User struct{ ID int } `json:"user"`
		}
		if _, _, err := api.Read(ctx, fmt.Sprintf("/repos/%s/issues/%d/comments?per_page=100&page=%d", cfg.Repository, issue, page), "", &comments); err != nil {
			return nil, markerConflicts{}, err
		}
		for _, comment := range comments {
			if !strings.Contains(comment.Body, activeMarkerPrefix) {
				continue
			}
			marker, err := parseActiveAttemptMarker(comment.Body)
			branch, branchErr := AttemptBranch(cfg.Repository, marker.Issue, marker.Attempt)
			trusted := comment.User.ID == cfg.ActorID && cfg.ActorID > 0
			if err != nil || marker.Issue != issue || branchErr != nil || marker.Branch != branch || !trusted {
				conflicts.Any = true
				continue
			}
			if previous, ok := found[marker.Attempt]; ok && previous != marker {
				conflicts.Any = true
				conflicts.Attempts[marker.Attempt] = true
				continue
			}
			found[marker.Attempt] = marker
		}
		if len(comments) < 100 {
			markers := make([]activeMarkerPayload, 0, len(found))
			for _, marker := range found {
				markers = append(markers, marker)
			}
			slices.SortFunc(markers, func(a, b activeMarkerPayload) int { return a.Attempt - b.Attempt })
			return markers, conflicts, nil
		}
	}
	return nil, markerConflicts{}, errors.New("issue comments exceed bounded recovery limit")
}

func EnsureActiveAttempt(ctx context.Context, api API, cfg PRAdapterConfig, issue, attempt int, baseSHA string) error {
	marker, err := ActiveAttemptMarker(cfg.Repository, issue, attempt, baseSHA)
	if err != nil {
		return err
	}
	check := func() (bool, error) {
		found, conflicts, err := fetchActiveAttempts(ctx, api, cfg, issue)
		if err != nil {
			return false, err
		}
		if conflicts.Any {
			return false, errors.New("active attempt marker conflicts with dispatch")
		}
		present := false
		for _, marker := range found {
			if marker.Attempt > attempt || marker.Attempt == attempt && marker.BaseSHA != baseSHA {
				return false, errors.New("active attempt marker conflicts with dispatch")
			}
			present = present || marker.Attempt == attempt
		}
		return present, nil
	}
	if present, err := check(); err != nil || present {
		return err
	}
	message, _ := AttributedBody(issue, attempt, "Attempt bound for execution.")
	createErr := api.CreateIssueComment(ctx, cfg.Repository, issue, message+"\n\n"+marker, Mutation{Issue: issue, Attempt: attempt})
	present, confirmErr := check()
	if confirmErr != nil {
		return confirmErr
	}
	if present {
		return nil
	}
	if createErr != nil {
		return createErr
	}
	return errors.New("active attempt marker creation was not observable")
}

func retryAuthorizesFailure(controls Controls, retry *Provenance, terminal terminalMarkerPayload) bool {
	return controls.Retry && retry != nil && retry.CreatedAt.After(terminal.FailedAt)
}

// EnsureRetryCommand records the one exact control that permits a new attempt.
// It is intentionally separate from attributed implementation comments because
// control parsing requires the entire comment body to be the command.
func EnsureRetryCommand(ctx context.Context, api API, cfg PRAdapterConfig, issue, attempt int) error {
	if cfg.Repository == "" || cfg.ActorID <= 0 || cfg.RetryCommand == "" || issue < 1 || attempt < 1 {
		return errors.New("retry command requires repository, actor, issue, and attempt")
	}
	terminals, terminalConflicts, err := fetchTerminalFailures(ctx, api, cfg, issue)
	if err != nil {
		return err
	}
	bindings, bindingConflicts, err := fetchActiveAttempts(ctx, api, cfg, issue)
	if err != nil {
		return err
	}
	if len(terminals) == 0 {
		return errors.New("retry requires an exact paired terminal attempt")
	}
	terminal := terminals[len(terminals)-1]
	terminalAttempts := make(map[int]bool, len(terminals))
	for _, marker := range terminals {
		terminalAttempts[marker.Attempt] = true
	}
	paired := false
	unsafeBinding := false
	for _, binding := range bindings {
		paired = paired || binding.Attempt == attempt
		unsafeBinding = unsafeBinding || binding.Attempt > attempt || binding.Attempt != attempt && !terminalAttempts[binding.Attempt]
	}
	if terminalConflicts.Any || bindingConflicts.Any || terminal.Attempt != attempt || !paired || unsafeBinding {
		return errors.New("retry requires an exact paired terminal attempt")
	}
	source := GitHubPRSource{API: api, Config: cfg}
	permission, err := source.actorPermission(ctx, cfg.ActorID)
	if err != nil {
		return err
	}
	if permission != "maintain" && permission != "admin" {
		return errors.New("authenticated coordinator requires maintain or admin permission to retry")
	}
	check := func() (bool, error) {
		comments, err := source.issueComments(ctx, issue)
		if err != nil {
			return false, err
		}
		latest, name := latestControlCommand(comments, cfg.CancelCommand, cfg.RetryCommand)
		return latest != nil && name == "retry" && latest.User.ID == cfg.ActorID && latest.CreatedAt.After(terminal.FailedAt), nil
	}
	if present, err := check(); err != nil || present {
		return err
	}
	body, _ := json.Marshal(map[string]string{"body": cfg.RetryCommand})
	resp, mutationErr := api.do(ctx, http.MethodPost, fmt.Sprintf("/repos/%s/issues/%d/comments", cfg.Repository, issue), "", body, Mutation{Issue: issue, Attempt: attempt})
	ambiguous := mutationErr != nil
	if resp != nil {
		defer resp.Body.Close()
	}
	if mutationErr == nil && resp == nil {
		mutationErr = errors.New("GitHub retry command returned no response")
	} else if mutationErr == nil && (resp.StatusCode < 200 || resp.StatusCode >= 300) {
		mutationErr = responseError("GitHub retry command", resp)
	}
	present, readErr := check()
	if readErr != nil {
		return readErr
	}
	if present {
		return nil
	}
	if mutationErr != nil {
		if !ambiguous {
			return mutationErr
		}
		return &ambiguousMutationError{fmt.Errorf("GitHub retry command outcome is ambiguous; reconcile issue #%d attempt %d: %w", issue, attempt, mutationErr)}
	}
	return errors.New("GitHub retry command creation was not observable")
}

func fetchTerminalFailures(ctx context.Context, api API, cfg PRAdapterConfig, issue int) ([]terminalMarkerPayload, markerConflicts, error) {
	found := map[int]terminalMarkerPayload{}
	conflicts := markerConflicts{Attempts: map[int]bool{}}
	for page := 1; page <= recoveryPageLimit; page++ {
		var comments []struct {
			Body string `json:"body"`
			User struct {
				ID int `json:"id"`
			} `json:"user"`
		}
		if _, _, err := api.Read(ctx, fmt.Sprintf("/repos/%s/issues/%d/comments?per_page=100&page=%d", cfg.Repository, issue, page), "", &comments); err != nil {
			return nil, markerConflicts{}, err
		}
		for _, comment := range comments {
			if !strings.Contains(comment.Body, terminalMarkerPrefix) || comment.User.ID != cfg.ActorID || cfg.ActorID <= 0 {
				continue
			}
			marker, err := parseTerminalMarker(comment.Body)
			if err != nil || marker.Issue != issue {
				conflicts.Any = true
				continue
			}
			if previous, ok := found[marker.Attempt]; ok && previous != marker {
				conflicts.Any = true
				conflicts.Attempts[marker.Attempt] = true
				continue
			}
			found[marker.Attempt] = marker
		}
		if len(comments) < 100 {
			markers := make([]terminalMarkerPayload, 0, len(found))
			for _, marker := range found {
				markers = append(markers, marker)
			}
			slices.SortFunc(markers, func(a, b terminalMarkerPayload) int {
				if a.FailedAt.Before(b.FailedAt) {
					return -1
				}
				if a.FailedAt.After(b.FailedAt) {
					return 1
				}
				return a.Attempt - b.Attempt
			})
			return markers, conflicts, nil
		}
	}
	return nil, markerConflicts{}, errors.New("issue comments exceed bounded recovery limit")
}

func FetchAttemptFacts(ctx context.Context, api API, repository string, actorID int) ([]RecoveryAttemptFact, error) {
	return fetchAttemptFacts(ctx, api, repository, actorID, 0, 0, true)
}

// FetchOperatorAttemptFacts reads only the deterministic pull request for one
// operator-message target and skips check data that cannot affect delivery.
func FetchOperatorAttemptFacts(ctx context.Context, api API, repository string, actorID, issue, attempt int) ([]RecoveryAttemptFact, error) {
	if issue < 1 || attempt < 1 {
		return nil, errors.New("operator attempt lookup requires a positive issue and attempt")
	}
	return fetchAttemptFacts(ctx, api, repository, actorID, issue, attempt, false)
}

func fetchAttemptFacts(ctx context.Context, api API, repository string, actorID, targetIssue, targetAttempt int, includeChecks bool) ([]RecoveryAttemptFact, error) {
	var facts []RecoveryAttemptFact
	for page := 1; ; page++ {
		var pulls []struct {
			Number      int `json:"number"`
			Body, State string
			MergedAt    any `json:"merged_at"`
			User        struct {
				ID int `json:"id"`
			} `json:"user"`
			Head struct {
				SHA, Ref string
			} `json:"head"`
			Base struct {
				SHA string `json:"sha"`
			} `json:"base"`
		}
		path := fmt.Sprintf("/repos/%s/pulls?state=all&sort=updated&direction=desc&per_page=%d&page=%d", repository, recoveryPullsPerPage, page)
		if targetIssue > 0 {
			branch, err := AttemptBranch(repository, targetIssue, targetAttempt)
			parts := strings.Split(repository, "/")
			if err != nil || len(parts) != 2 {
				return nil, errors.New("operator attempt lookup requires a valid repository binding")
			}
			path = fmt.Sprintf("/repos/%s/pulls?state=all&head=%s&per_page=%d&page=%d", repository, url.QueryEscape(parts[0]+":"+branch), recoveryPullsPerPage, page)
		}
		if _, _, err := api.Read(ctx, path, "", &pulls); err != nil {
			return nil, err
		}
		for _, pull := range pulls {
			marker, markerErr := parseAttemptMarker(pull.Body)
			if markerErr != nil {
				continue
			}
			issue, attempt := marker.Issue, marker.Attempt
			if targetIssue > 0 && (issue != targetIssue || attempt != targetAttempt) {
				continue
			}
			wantBranch, err := AttemptBranch(repository, issue, attempt)
			if err != nil || marker.Branch != wantBranch || marker.Branch != pull.Head.Ref || marker.Head != pull.Head.SHA || marker.PR != pull.Number || pull.User.ID != actorID {
				continue
			}
			if pull.Number < 1 || issue < 1 || attempt < 1 || pull.Head.SHA == "" || pull.Base.SHA == "" {
				return nil, errors.New("marked pull request has incomplete identity")
			}
			var currentIssue struct {
				State  string
				Labels []struct{ Name string }
			}
			if _, _, err := api.Read(ctx, fmt.Sprintf("/repos/%s/issues/%d", repository, issue), "", &currentIssue); err != nil {
				return nil, err
			}
			var comments []struct {
				Body string `json:"body"`
				User struct {
					ID int `json:"id"`
				} `json:"user"`
			}
			for commentPage := 1; commentPage <= recoveryPageLimit; commentPage++ {
				var page []struct {
					Body string `json:"body"`
					User struct {
						ID int `json:"id"`
					} `json:"user"`
				}
				if _, _, err := api.Read(ctx, fmt.Sprintf("/repos/%s/issues/%d/comments?per_page=100&page=%d", repository, issue, commentPage), "", &page); err != nil {
					return nil, err
				}
				comments = append(comments, page...)
				if len(page) < 100 {
					break
				}
				if commentPage == recoveryPageLimit {
					return nil, errors.New("issue comments exceed bounded recovery limit")
				}
			}
			authoritative := false
			boundBase := ""
			baseConflict := false
			for _, comment := range comments {
				if comment.User.ID != actorID {
					continue
				}
				got, err := parseAttemptMarker(comment.Body)
				if err == nil && got == marker {
					authoritative = true
				}
				binding, err := parseActiveAttemptMarker(comment.Body)
				if err == nil && binding.Issue == issue && binding.Attempt == attempt && binding.Branch == wantBranch {
					baseConflict = boundBase != "" && boundBase != binding.BaseSHA
					boundBase = binding.BaseSHA
				}
			}
			if !authoritative || baseConflict {
				continue
			}
			if boundBase == "" { // Compatibility with attempts published before active bindings existed.
				boundBase = pull.Base.SHA
			}
			state := "active"
			if pull.MergedAt != nil {
				state = "completed"
			} else if pull.State != "open" || currentIssue.State == "closed" {
				state = "blocked"
			}
			var checks []string
			if includeChecks {
				var allRuns []struct{ Name, Status, Conclusion string }
				for checkPage := 1; checkPage <= recoveryPageLimit; checkPage++ {
					var runs struct {
						CheckRuns []struct{ Name, Status, Conclusion string } `json:"check_runs"`
					}
					if _, _, err := api.Read(ctx, fmt.Sprintf("/repos/%s/commits/%s/check-runs?filter=latest&per_page=100&page=%d", repository, pull.Head.SHA, checkPage), "", &runs); err != nil {
						return nil, err
					}
					allRuns = append(allRuns, runs.CheckRuns...)
					if len(runs.CheckRuns) < 100 {
						break
					}
					if checkPage == recoveryPageLimit {
						return nil, errors.New("check runs exceed bounded recovery limit")
					}
				}
				checksPass := len(allRuns) > 0
				for _, run := range allRuns {
					checks = append(checks, run.Name+":"+run.Status+":"+run.Conclusion)
					checksPass = checksPass && run.Status == "completed" && (run.Conclusion == "success" || run.Conclusion == "neutral" || run.Conclusion == "skipped")
				}
				var statuses struct {
					Statuses []struct{ Context, State string }
				}
				if _, _, err := api.Read(ctx, fmt.Sprintf("/repos/%s/commits/%s/status", repository, pull.Head.SHA), "", &statuses); err != nil {
					return nil, err
				}
				checksPass = checksPass || len(statuses.Statuses) > 0
				for _, status := range statuses.Statuses {
					checks = append(checks, status.Context+":"+status.State)
					checksPass = checksPass && status.State == "success"
				}
				if state == "active" && checksPass {
					state = "review-ready"
				}
			}
			facts = append(facts, RecoveryAttemptFact{Repository: repository, Issue: issue, Attempt: attempt, BaseSHA: boundBase, HeadSHA: pull.Head.SHA, PR: pull.Number, State: state, PublicationConfirmed: true, Checks: checks})
		}
		if targetIssue > 0 || len(pulls) < recoveryPullsPerPage {
			return facts, nil
		}
	}
}

// FindPublishedAttempt reconstructs publication even while the PR body or issue
// comment is still missing after a crash.
func FindPublishedAttempt(ctx context.Context, api API, repository, branch, head string, actorID int) (PullRequest, string, error) {
	var found PullRequest
	var body string
	for page := 1; page <= recoveryPageLimit; page++ {
		var pulls []struct {
			Number int    `json:"number"`
			Body   string `json:"body"`
			User   struct {
				ID int `json:"id"`
			} `json:"user"`
			Head struct{ SHA, Ref string } `json:"head"`
		}
		if _, _, err := api.Read(ctx, fmt.Sprintf("/repos/%s/pulls?state=all&sort=updated&direction=desc&per_page=%d&page=%d", repository, recoveryPullsPerPage, page), "", &pulls); err != nil {
			return found, "", err
		}
		for _, pull := range pulls {
			if pull.Head.Ref != branch || pull.Head.SHA != head {
				continue
			}
			if pull.User.ID != actorID {
				return found, "", errors.New("deterministic attempt branch is owned by an untrusted pull request")
			}
			if found.Number != 0 && found.Number != pull.Number {
				return found, "", errors.New("multiple pull requests exist for deterministic attempt head")
			}
			found, body = PullRequest{Number: pull.Number}, pull.Body
		}
		if len(pulls) < recoveryPullsPerPage {
			return found, body, nil
		}
	}
	return found, body, errors.New("pull requests exceed bounded recovery limit")
}

func HasAttemptComment(ctx context.Context, api API, repository string, issue int, marker string, actorID int) (bool, error) {
	for page := 1; page <= recoveryPageLimit; page++ {
		var comments []struct {
			Body string `json:"body"`
			User struct {
				ID int `json:"id"`
			} `json:"user"`
		}
		if _, _, err := api.Read(ctx, fmt.Sprintf("/repos/%s/issues/%d/comments?per_page=100&page=%d", repository, issue, page), "", &comments); err != nil {
			return false, err
		}
		for _, comment := range comments {
			if strings.Contains(comment.Body, marker) && comment.User.ID == actorID {
				return true, nil
			}
		}
		if len(comments) < 100 {
			return false, nil
		}
	}
	return false, errors.New("issue comments exceed bounded recovery limit")
}
