package github

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"
)

const recoveryMarkerPrefix = "<!-- agent-symphony:attempt:v1\n"
const terminalMarkerPrefix = "<!-- agent-symphony:terminal:v1\n"
const activeMarkerPrefix = "<!-- agent-symphony:active-attempt:v1\n"
const recoveryPageLimit = 10

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
	start := strings.LastIndex(body, terminalMarkerPrefix)
	if start < 0 || len(body) > 64<<10 {
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
	ActiveAttempt                                 *RecoveryAttemptFact
}

// FetchIssueFacts returns the authorized issue-control projection used by both
// scheduling and read-only status. Intake permits the reconciliation command
// to create a missing control snapshot; status calls remain read-only.
func FetchIssueFacts(ctx context.Context, api API, cfg PRAdapterConfig, attempts []RecoveryAttemptFact, intake bool) ([]RecoveryIssueFact, error) {
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
			Number      int
			Title, Body string
			CreatedAt   time.Time `json:"created_at"`
			PullRequest any       `json:"pull_request"`
		}
		if _, _, err := api.Read(ctx, fmt.Sprintf("/repos/%s/issues?state=open&per_page=100&page=%d", cfg.Repository, page), "", &issues); err != nil {
			return nil, err
		}
		for _, issue := range issues {
			if issue.PullRequest != nil {
				continue
			}
			bindings, bindingConflict, err := fetchActiveAttempts(ctx, api, cfg, issue.Number)
			if err != nil {
				return nil, err
			}
			terminal, err := fetchTerminalFailure(ctx, api, cfg, issue.Number)
			if err != nil {
				return nil, err
			}
			var binding activeMarkerPayload
			for _, candidate := range bindings {
				bindingFinal := slices.ContainsFunc(attempts, func(attempt RecoveryAttemptFact) bool {
					return attempt.Issue == issue.Number && attempt.Attempt == candidate.Attempt
				})
				if terminal.Attempt >= candidate.Attempt || bindingFinal {
					continue
				}
				if binding.Attempt != 0 {
					bindingConflict = true
					continue
				}
				binding = candidate
			}
			if binding.Attempt >= next[issue.Number] {
				next[issue.Number] = binding.Attempt + 1
			}
			if terminal.Attempt >= next[issue.Number] {
				next[issue.Number] = terminal.Attempt + 1
			}
			controls, _, retry, err := source.authorizedControlsWithIntake(ctx, issue.Number, intake)
			if err != nil {
				attempt := max(1, next[issue.Number])
				var activeAttempt *RecoveryAttemptFact
				if binding.Attempt > 0 {
					attempt = binding.Attempt
					activeAttempt = &RecoveryAttemptFact{Repository: cfg.Repository, Issue: issue.Number, Attempt: binding.Attempt, BaseSHA: binding.BaseSHA, State: "active"}
				}
				result = append(result, RecoveryIssueFact{Repository: cfg.Repository, Issue: issue.Number, Attempt: attempt, Title: issue.Title, Body: issue.Body, BaseSHA: branch.Commit.SHA, BaseBranch: repository.DefaultBranch, CreatedAt: issue.CreatedAt, Blockers: []string{err.Error()}, Active: active[issue.Number] || binding.Attempt > 0 || bindingConflict, Completed: completed[issue.Number], ActiveAttempt: activeAttempt})
				continue
			}
			blockers := []string{}
			if bindingConflict {
				blockers = append(blockers, "active attempt marker is foreign, malformed, or contradictory")
			}
			for _, dependency := range controls.Dependencies {
				if !completed[dependency] {
					blockers = append(blockers, fmt.Sprintf("dependency #%d is incomplete", dependency))
				}
			}
			if terminal.Attempt > 0 && !retryAuthorizesFailure(controls, retry, terminal) {
				blockers = append(blockers, fmt.Sprintf("attempt %d has a coordinator-authored terminal failure requiring a later authorized retry", terminal.Attempt))
			}
			authorized := controls.Ready && !controls.Closed && !controls.Cancelled && len(blockers) == 0
			bound := binding.Attempt > 0
			attempt := max(1, next[issue.Number])
			var activeAttempt *RecoveryAttemptFact
			if bound {
				attempt = binding.Attempt
				activeAttempt = &RecoveryAttemptFact{Repository: cfg.Repository, Issue: issue.Number, Attempt: binding.Attempt, BaseSHA: binding.BaseSHA, State: "active"}
			}
			isActive := active[issue.Number] || bound || bindingConflict
			eligible := authorized && !isActive && !completed[issue.Number]
			result = append(result, RecoveryIssueFact{Repository: cfg.Repository, Issue: issue.Number, Attempt: attempt, Title: issue.Title, Body: issue.Body, BaseSHA: branch.Commit.SHA, BaseBranch: repository.DefaultBranch, CreatedAt: issue.CreatedAt, Priority: controls.Priority, Dependencies: controls.Dependencies, Paths: IssuePaths(issue.Body), Blockers: blockers, Eligible: eligible, Active: isActive, Completed: completed[issue.Number], Retry: controls.Retry, Cancelled: controls.Cancelled, DispatchAuthorized: authorized, ActiveAttempt: activeAttempt})
		}
		if len(issues) < 100 {
			return result, nil
		}
	}
	return nil, errors.New("open issues exceed bounded recovery limit")
}

func fetchActiveAttempts(ctx context.Context, api API, cfg PRAdapterConfig, issue int) ([]activeMarkerPayload, bool, error) {
	found := map[int]activeMarkerPayload{}
	conflict := false
	for page := 1; page <= recoveryPageLimit; page++ {
		var comments []struct {
			Body string           `json:"body"`
			User struct{ ID int } `json:"user"`
		}
		if _, _, err := api.Read(ctx, fmt.Sprintf("/repos/%s/issues/%d/comments?per_page=100&page=%d", cfg.Repository, issue, page), "", &comments); err != nil {
			return nil, false, err
		}
		for _, comment := range comments {
			if !strings.Contains(comment.Body, activeMarkerPrefix) {
				continue
			}
			marker, err := parseActiveAttemptMarker(comment.Body)
			branch, branchErr := AttemptBranch(cfg.Repository, marker.Issue, marker.Attempt)
			trusted := comment.User.ID == cfg.ActorID && cfg.ActorID > 0
			if err != nil || marker.Issue != issue || branchErr != nil || marker.Branch != branch || !trusted {
				conflict = true
				continue
			}
			if previous, ok := found[marker.Attempt]; ok && previous != marker {
				conflict = true
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
			return markers, conflict, nil
		}
	}
	return nil, false, errors.New("issue comments exceed bounded recovery limit")
}

func EnsureActiveAttempt(ctx context.Context, api API, cfg PRAdapterConfig, issue, attempt int, baseSHA string) error {
	marker, err := ActiveAttemptMarker(cfg.Repository, issue, attempt, baseSHA)
	if err != nil {
		return err
	}
	check := func() (bool, error) {
		found, conflict, err := fetchActiveAttempts(ctx, api, cfg, issue)
		if err != nil {
			return false, err
		}
		if conflict {
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

func fetchTerminalFailure(ctx context.Context, api API, cfg PRAdapterConfig, issue int) (terminalMarkerPayload, error) {
	var latest terminalMarkerPayload
	for page := 1; page <= recoveryPageLimit; page++ {
		var comments []struct {
			Body string `json:"body"`
			User struct {
				ID int `json:"id"`
			} `json:"user"`
		}
		if _, _, err := api.Read(ctx, fmt.Sprintf("/repos/%s/issues/%d/comments?per_page=100&page=%d", cfg.Repository, issue, page), "", &comments); err != nil {
			return terminalMarkerPayload{}, err
		}
		for _, comment := range comments {
			if marker, err := parseTerminalMarker(comment.Body); err == nil && marker.Issue == issue && comment.User.ID == cfg.ActorID {
				if marker.FailedAt.After(latest.FailedAt) || marker.FailedAt.Equal(latest.FailedAt) && marker.Attempt > latest.Attempt {
					latest = marker
				}
			}
		}
		if len(comments) < 100 {
			return latest, nil
		}
	}
	return terminalMarkerPayload{}, errors.New("issue comments exceed bounded recovery limit")
}

func FetchAttemptFacts(ctx context.Context, api API, repository string, actorID int) ([]RecoveryAttemptFact, error) {
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
		if _, _, err := api.Read(ctx, fmt.Sprintf("/repos/%s/pulls?state=all&sort=updated&direction=desc&per_page=100&page=%d", repository, page), "", &pulls); err != nil {
			return nil, err
		}
		for _, pull := range pulls {
			marker, markerErr := parseAttemptMarker(pull.Body)
			if markerErr != nil {
				continue
			}
			issue, attempt := marker.Issue, marker.Attempt
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
			checks := make([]string, 0, len(allRuns))
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
			facts = append(facts, RecoveryAttemptFact{Repository: repository, Issue: issue, Attempt: attempt, BaseSHA: boundBase, HeadSHA: pull.Head.SHA, PR: pull.Number, State: state, Checks: checks})
		}
		if len(pulls) < 100 {
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
		if _, _, err := api.Read(ctx, fmt.Sprintf("/repos/%s/pulls?state=all&sort=updated&direction=desc&per_page=100&page=%d", repository, page), "", &pulls); err != nil {
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
		if len(pulls) < 100 {
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
