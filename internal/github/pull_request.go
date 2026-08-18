package github

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"time"
)

const PolicyCheck = "agent-symphony/policy"

type FeedbackState string

const (
	FeedbackPending   FeedbackState = "pending"
	FeedbackAddressed FeedbackState = "addressed"
	FeedbackBlocked   FeedbackState = "blocked"
)

type FeedbackExecutionState string

const (
	FeedbackClaimed   FeedbackExecutionState = "claimed"
	FeedbackInFlight  FeedbackExecutionState = "in-flight"
	FeedbackCompleted FeedbackExecutionState = "completed"
)

type Feedback struct {
	ID         int64
	Source     string
	ActorID    int
	Body       string
	CreatedAt  time.Time
	State      FeedbackState
	Execution  FeedbackExecutionState
	Authorized bool
	Delegated  bool
	Evidence   string
}

type PRFacts struct {
	IssueOpen, IssueEligible, AutonomousMerge, PRIsOpen, Draft, Mergeable, Behind, BaseRequiresCurrent bool
	HeadSHA, ValidationSHA, DocumentationSHA                                                           string
	NeedsHumanReview, ApprovalRequired, Approved, ChangesRequested                                     bool
	CodeOwnerApprovalRequired, CodeOwnerApproved, LastPushApprovalRequired, LastPushApproved           bool
	RequiredChecksPass, PolicyCheckRequired, MergePermission, BranchProtectionAllows                   bool
	ConflictingScope, BranchModifiedOutsideAttempt                                                     bool
	Feedback                                                                                           []Feedback
}

type PolicyResult struct {
	CheckStatus, CheckConclusion string
	Reasons, MergeBlockers       []string
	Merge                        bool
}

// PRState is reconstructed by PRSource from GitHub and issue #4's recovered
// attempt state on every reconciliation. It is not coordinator-owned state.
type PRState struct {
	Repository, HeadSHA, CheckHead, PolicyStatus, ValidationQueuedSHA, ValidationInFlightSHA string
	ValidationGeneration                                                                     uint64 `json:"validation_generation,omitempty"`
	ValidationResult, ValidationEvidence                                                     string
	MergeAttemptSHA, MergePhase                                                              string
	Number, Issue, Attempt                                                                   int
	ReviewLabelPresent                                                                       bool
	Facts                                                                                    PRFacts
	Decisions                                                                                []Decision
	PendingDispositions                                                                      []Feedback
	ConfirmedDispositions                                                                    []Feedback           `json:"-"`
	HandoffReceipts                                                                          map[string]bool      `json:"handoff_receipts,omitempty"`
	PreparedPublication                                                                      *PreparedPublication `json:"prepared_publication,omitempty"`
}

type Decision struct {
	ID, Body string
	Recorded bool
}

type PRSource interface {
	OpenPullRequests(context.Context) ([]int, error)
	FreshPullRequest(context.Context, int) (PRState, error)
	FreshFeedback(context.Context, PRState, Feedback) (Feedback, error)
}

type Reconciler struct {
	FullRead     func() error
	PullRequests *PRCoordinator
}

func (r Reconciler) RunOnce() error { return r.runOnce(context.Background()) }

func (r Reconciler) runOnce(ctx context.Context) error {
	if r.FullRead == nil {
		return errors.New("reconciliation read is required")
	}
	if err := r.FullRead(); err != nil {
		return err
	}
	if r.PullRequests != nil {
		return r.PullRequests.Reconcile(ctx)
	}
	return nil
}

// PRSignals hands work to the existing attempt/recovery owner. Implementations
// must durably deduplicate immutable feedback IDs before returning.
type PRSignals interface {
	DelegateFeedback(context.Context, PRState, Feedback) error
	RerunValidation(context.Context, PRState) error
}

type PRCoordinator struct {
	API         API
	Source      PRSource
	Signals     PRSignals
	ReviewLabel string
	MergeMethod string
	ActorID     int
}

// Reconcile discovers every open PR and derives all effects from fresh facts.
func (c PRCoordinator) Reconcile(ctx context.Context) error {
	if c.Source == nil || c.Signals == nil {
		return errors.New("pull request reconciliation source and recovery signals are required")
	}
	numbers, err := c.Source.OpenPullRequests(ctx)
	if err != nil {
		return err
	}
	var errs []error
	for _, number := range numbers {
		if err := c.reconcileOne(ctx, number); err != nil {
			errs = append(errs, fmt.Errorf("reconcile pull request %d: %w", number, err))
		}
	}
	return errors.Join(errs...)
}

func (c PRCoordinator) reconcileOne(ctx context.Context, number int) error {
	state, err := c.Source.FreshPullRequest(ctx, number)
	if err != nil {
		return err
	}
	attribution := Mutation{Issue: state.Issue, Attempt: state.Attempt}
	repository := state.Repository
	if !validPRState(state, number, repository, attribution) {
		return errors.New("contradictory pull request identity or head")
	}
	if state.MergeAttemptSHA == state.HeadSHA && state.MergePhase == "dispatched" {
		merged, err := c.API.PullRequestMerged(ctx, state.Repository, number)
		if err != nil || merged {
			return err
		}
		body, err := AttributedBody(state.Issue, state.Attempt, mergeDisposition(state.HeadSHA, "resolved"))
		if err != nil {
			return err
		}
		return c.API.CreateIssueComment(ctx, state.Repository, state.Issue, body, attribution)
	}
	if err := c.API.SyncReviewLabel(ctx, state.Repository, number, c.ReviewLabel, state.ReviewLabelPresent, state.Facts.NeedsHumanReview, attribution); err != nil {
		return err
	}
	seenDecisions := make(map[string]bool, len(state.Decisions))
	for _, decision := range state.Decisions {
		if decision.ID == "" || seenDecisions[decision.ID] || strings.TrimSpace(decision.Body) == "" || decision.Recorded {
			continue
		}
		seenDecisions[decision.ID] = true
		body, err := AttributedBody(state.Issue, state.Attempt, fmt.Sprintf("Implementation decision %s\n\n%s", decision.ID, decision.Body))
		if err != nil {
			return err
		}
		if err := c.API.CreateIssueComment(ctx, state.Repository, state.Issue, body, attribution); err != nil {
			return err
		}
	}
	for _, feedback := range state.PendingDispositions {
		body, err := FeedbackDispositionBody(state.Issue, state.Attempt, feedback, feedback.Evidence)
		if err != nil {
			return err
		}
		if err := c.API.CreateIssueComment(ctx, state.Repository, state.Issue, body, attribution); err != nil {
			return err
		}
	}
	for _, feedback := range ActionableFeedback(state.Facts.Feedback, func(int) bool { return true }) {
		if feedback.State == "" {
			feedback.State = FeedbackPending
		}
		if !feedback.Delegated {
			freshState, err := c.Source.FreshPullRequest(ctx, number)
			if err != nil || !validPRState(freshState, number, repository, attribution) || freshState.HeadSHA != state.HeadSHA {
				return errors.New("pull request identity changed before feedback delegation")
			}
			fresh, err := c.Source.FreshFeedback(ctx, freshState, feedback)
			if err != nil {
				return err
			}
			if fresh.identity() != feedback.identity() || fresh.ActorID != feedback.ActorID || fresh.Body != feedback.Body || !fresh.Authorized || strings.TrimSpace(fresh.Body) == "" {
				return fmt.Errorf("feedback %d identity or authorization changed before delegation", feedback.ID)
			}
			confirmedState, err := c.Source.FreshPullRequest(ctx, number)
			if err != nil || !validPRState(confirmedState, number, repository, attribution) || confirmedState.HeadSHA != state.HeadSHA {
				return errors.New("pull request identity changed immediately before feedback delegation")
			}
			if err := c.Signals.DelegateFeedback(ctx, confirmedState, fresh); err != nil {
				return err
			}
		}
	}
	if hasAddressedFeedback(state.Facts.Feedback) && state.Facts.ValidationSHA != state.HeadSHA && state.ValidationQueuedSHA != state.HeadSHA {
		if err := c.Signals.RerunValidation(ctx, state); err != nil {
			return err
		}
	}
	state, err = c.Source.FreshPullRequest(ctx, number)
	if err != nil {
		return err
	}
	if !validPRState(state, number, repository, attribution) {
		return errors.New("pull request identity changed during reconciliation")
	}
	result := EvaluatePR(state.Facts)
	if status := policyStatus(result); state.CheckHead != state.HeadSHA || state.PolicyStatus != status {
		if err := c.API.PublishPolicyStatus(ctx, state.Repository, state.HeadSHA, result, attribution); err != nil {
			return err
		}
	}
	if !result.Merge && result.CheckStatus == "completed" && c.ActorID > 0 {
		body, err := PolicyFailureBody(state.Issue, state.Attempt, state.HeadSHA, state.ValidationResult, result.MergeBlockers)
		if err != nil {
			return err
		}
		present, err := HasAttemptComment(ctx, c.API, state.Repository, state.Number, body, c.ActorID)
		if err != nil {
			return err
		}
		if !present {
			if err := c.API.CreateIssueComment(ctx, state.Repository, state.Number, body, attribution); err != nil {
				return err
			}
		}
	}
	if !result.Merge {
		return nil
	}
	evaluatedHead := state.HeadSHA
	state, err = c.Source.FreshPullRequest(ctx, number)
	if err != nil {
		return err
	}
	if !validPRState(state, number, repository, attribution) || state.HeadSHA != evaluatedHead {
		return nil
	}
	if result = EvaluatePR(state.Facts); !result.Merge {
		return nil
	}
	if state.MergeAttemptSHA != state.HeadSHA || state.MergePhase != "prepared" {
		prepared, err := AttributedBody(state.Issue, state.Attempt, mergeDisposition(state.HeadSHA, "prepared"))
		if err != nil {
			return err
		}
		if err := c.API.CreateIssueComment(ctx, state.Repository, state.Issue, prepared, attribution); err != nil {
			return err
		}
	}
	dispatched, err := AttributedBody(state.Issue, state.Attempt, mergeDisposition(state.HeadSHA, "dispatched"))
	if err != nil {
		return err
	}
	if err := c.API.CreateIssueComment(ctx, state.Repository, state.Issue, dispatched, attribution); err != nil {
		return err
	}
	err = c.API.MergePullRequest(ctx, state.Repository, number, state.HeadSHA, c.MergeMethod, attribution)
	if err == nil || IsAmbiguousMutation(err) {
		return err
	}
	body, bodyErr := AttributedBody(state.Issue, state.Attempt, mergeDisposition(state.HeadSHA, "resolved"))
	if bodyErr != nil {
		return errors.Join(err, bodyErr)
	}
	return errors.Join(err, c.API.CreateIssueComment(ctx, state.Repository, state.Issue, body, attribution))
}

func mergeDisposition(head, state string) string {
	return fmt.Sprintf("Merge attempt for head `%s`: **%s**.", head, state)
}

func validPRState(state PRState, number int, repository string, attribution Mutation) bool {
	return state.Number == number && state.Repository != "" && state.Repository == repository && state.HeadSHA != "" && state.Facts.HeadSHA == state.HeadSHA && state.Issue == attribution.Issue && state.Attempt == attribution.Attempt && state.Issue > 0 && state.Attempt > 0
}

func hasAddressedFeedback(feedback []Feedback) bool {
	return slices.ContainsFunc(feedback, func(f Feedback) bool { return f.Authorized && f.State == FeedbackAddressed })
}

// EvaluatePR derives the required Check and merge decision entirely from fresh GitHub facts.
func EvaluatePR(f PRFacts) PolicyResult {
	var reasons, mergeBlockers []string
	if f.HeadSHA == "" || f.ValidationSHA != f.HeadSHA {
		reasons = append(reasons, "validation evidence is missing or stale")
	}
	if f.HeadSHA == "" || f.DocumentationSHA != f.HeadSHA {
		reasons = append(reasons, "documentation impact assessment is missing or stale")
	}
	if f.NeedsHumanReview {
		reasons = append(reasons, "human review is required")
	}
	for _, feedback := range f.Feedback {
		if feedback.Authorized && (feedback.State == "" || feedback.State == FeedbackPending) && (feedback.Execution == FeedbackClaimed || feedback.Execution == FeedbackInFlight) {
			reasons = append(reasons, fmt.Sprintf("feedback %s execution is %s", feedback.identity(), feedback.Execution))
		}
		if feedback.Authorized && (feedback.State == FeedbackPending || feedback.State == FeedbackBlocked || feedback.State == "") {
			state := feedback.State
			if state == "" {
				state = FeedbackPending
			}
			reasons = append(reasons, fmt.Sprintf("feedback %d is %s", feedback.ID, state))
		}
	}
	mergeBlockers = append(mergeBlockers, reasons...)
	if !f.IssueOpen || !f.IssueEligible {
		mergeBlockers = append(mergeBlockers, "originating issue is not open and eligible")
	}
	if !f.PRIsOpen || f.Draft || !f.Mergeable {
		mergeBlockers = append(mergeBlockers, "pull request is not open, non-draft, and mergeable")
	}
	if f.ApprovalRequired && !f.Approved || f.ChangesRequested || f.CodeOwnerApprovalRequired && !f.CodeOwnerApproved || f.LastPushApprovalRequired && !f.LastPushApproved {
		mergeBlockers = append(mergeBlockers, "required review approval is not satisfied")
	}
	if !f.RequiredChecksPass {
		mergeBlockers = append(mergeBlockers, "required checks have not passed")
	}
	if f.BaseRequiresCurrent && f.Behind {
		mergeBlockers = append(mergeBlockers, "branch is behind its required base")
	}
	if f.ConflictingScope {
		mergeBlockers = append(mergeBlockers, "another active attempt has conflicting path scope")
	}
	if f.BranchModifiedOutsideAttempt {
		mergeBlockers = append(mergeBlockers, "attempt branch was modified outside the attempt")
	}
	if !f.MergePermission {
		mergeBlockers = append(mergeBlockers, "merge permission is unavailable")
	}
	result := PolicyResult{CheckStatus: "completed", Reasons: reasons, MergeBlockers: mergeBlockers}
	if f.NeedsHumanReview {
		result.CheckStatus = "in_progress"
	} else if len(mergeBlockers) == 0 {
		result.CheckConclusion = "success"
	} else {
		result.CheckConclusion = "failure"
	}
	result.Merge = f.AutonomousMerge && len(mergeBlockers) == 0
	return result
}

// ActionableFeedback returns authorized, unresolved feedback in stable lifetime order.
func ActionableFeedback(feedback []Feedback, authorized func(int) bool) []Feedback {
	if authorized == nil {
		return nil
	}
	result := slices.Clone(feedback)
	slices.SortFunc(result, func(a, b Feedback) int {
		if n := a.CreatedAt.Compare(b.CreatedAt); n != 0 {
			return n
		}
		return cmp.Compare(a.identity(), b.identity())
	})
	seen := make(map[string]bool, len(result))
	result = slices.DeleteFunc(result, func(f Feedback) bool {
		identity := f.identity()
		duplicate := seen[identity]
		seen[identity] = true
		return f.ID <= 0 || duplicate || strings.TrimSpace(f.Body) == "" || f.State == FeedbackAddressed || f.State == FeedbackBlocked || f.Execution != "" || !f.Authorized || !authorized(f.ActorID)
	})
	return result
}

func (f Feedback) identity() string {
	source := f.Source
	if source == "" {
		source = feedbackInline
	}
	return fmt.Sprintf("%s:%d", source, f.ID)
}

func PullRequestBody(issue, attempt int, validation, documentation, decisions string) (string, error) {
	if strings.TrimSpace(validation) == "" || strings.TrimSpace(documentation) == "" {
		return "", errors.New("pull request requires validation evidence and documentation impact")
	}
	body := fmt.Sprintf("Closes #%d\n\nAttempt: %d\n\n## Validation\n%s\n\n## Documentation impact\n%s", issue, attempt, validation, documentation)
	if strings.TrimSpace(decisions) != "" {
		body += "\n\n## Implementation decisions\n" + decisions
	}
	attributed, err := AttributedBody(issue, attempt, body)
	if err != nil {
		return "", err
	}
	return attributed, nil
}

// BindPullRequestBody adds the authoritative marker only after GitHub has
// assigned the PR number and the published head is known.
func BindPullRequestBody(body string, issue, attempt int, branch, head string, pr int) (string, error) {
	marker, err := AttemptMarker(issue, attempt, branch, head, pr, "review")
	if err != nil {
		return "", err
	}
	return body + "\n\n" + marker, nil
}

type PullRequest struct {
	Number int    `json:"number"`
	URL    string `json:"html_url"`
}

func (a API) CreatePullRequest(ctx context.Context, repository, title, head, base, body string, attribution Mutation) (PullRequest, error) {
	if repository == "" || title == "" || head == "" || base == "" {
		return PullRequest{}, errors.New("pull request repository, title, head, and base are required")
	}
	var pr PullRequest
	err := a.Mutate(ctx, http.MethodPost, "/repos/"+repository+"/pulls", map[string]any{"title": title, "head": head, "base": base, "body": body}, attribution, &pr)
	return pr, err
}

func (a API) UpdatePullRequest(ctx context.Context, repository string, number int, body string, attribution Mutation) error {
	return a.Mutate(ctx, http.MethodPatch, fmt.Sprintf("/repos/%s/pulls/%d", repository, number), map[string]string{"body": body}, attribution, nil)
}

func (a API) CreateIssueComment(ctx context.Context, repository string, number int, body string, attribution Mutation) error {
	return a.Mutate(ctx, http.MethodPost, fmt.Sprintf("/repos/%s/issues/%d/comments", repository, number), map[string]string{"body": body}, attribution, nil)
}

func (a API) createControlSnapshot(ctx context.Context, repository string, issue int, body string) error {
	if repository == "" || issue <= 0 {
		return errors.New("control snapshot requires repository and issue attribution")
	}
	if _, err := ParseSnapshotComment(body, 1, 1); err != nil {
		return err
	}
	b, _ := json.Marshal(map[string]string{"body": body})
	resp, err := a.do(ctx, http.MethodPost, fmt.Sprintf("/repos/%s/issues/%d/comments", repository, issue), "", b, Mutation{Issue: issue})
	if err != nil {
		return &ambiguousMutationError{fmt.Errorf("GitHub control snapshot outcome is ambiguous; reconcile issue #%d: %w", issue, err)}
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return responseError("GitHub control snapshot", resp)
	}
	return nil
}

// SyncReviewLabel applies only a known state transition, so reconciliation is idempotent.
func (a API) SyncReviewLabel(ctx context.Context, repository string, number int, label string, current, required bool, attribution Mutation) error {
	if label == "" || number <= 0 || current == required {
		return nil
	}
	if required {
		return a.mutateAttributed(ctx, http.MethodPost, fmt.Sprintf("/repos/%s/issues/%d/labels", repository, number), map[string][]string{"labels": {label}}, attribution)
	}
	resp, err := a.do(ctx, http.MethodDelete, fmt.Sprintf("/repos/%s/issues/%d/labels/%s", repository, number, url.PathEscape(label)), "", nil, attribution)
	if err != nil {
		return fmt.Errorf("GitHub mutation outcome is ambiguous; reconcile issue #%d attempt %d: %w", attribution.Issue, attribution.Attempt, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound || resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	return responseError("GitHub mutation", resp)
}

func FeedbackDispositionBody(issue, attempt int, feedback Feedback, detail string) (string, error) {
	if feedback.ID <= 0 || !slices.Contains([]FeedbackState{FeedbackPending, FeedbackAddressed, FeedbackBlocked}, feedback.State) {
		return "", errors.New("feedback disposition requires an ID and pending, addressed, or blocked state")
	}
	return AttributedBody(issue, attempt, fmt.Sprintf("Feedback %s: **%s**\n\n%s", feedback.identity(), feedback.State, strings.TrimSpace(detail)))
}

func EvidenceBody(issue, attempt int, kind, head string) (string, error) {
	if !slices.Contains([]string{"validation", "documentation"}, kind) || !regexpSHA.MatchString(head) {
		return "", errors.New("evidence requires validation or documentation and a commit SHA")
	}
	return AttributedBody(issue, attempt, fmt.Sprintf("Agent Symphony %s evidence for head `%s`.", kind, head))
}

func PolicyFailureBody(issue, attempt int, head, validation string, blockers []string) (string, error) {
	if !regexpSHA.MatchString(head) || len(blockers) == 0 || validation != "" && !slices.Contains([]string{"passed", "failed", "blocked"}, validation) {
		return "", errors.New("policy failure comment requires a head SHA and blockers")
	}
	blockers = slices.Clone(blockers)
	slices.Sort(blockers)
	blockers = slices.Compact(blockers)
	message := fmt.Sprintf("Agent Symphony could not resolve policy for head `%s`:\n\n- %s", head, strings.Join(blockers, "\n- "))
	if validation != "" {
		message += "\n\nThe remediation validation attempt ended as **" + validation + "**."
	}
	return AttributedBody(issue, attempt, message)
}

func (a API) PublishEvidence(ctx context.Context, repository string, issue int, kind, head string, attribution Mutation) error {
	body, err := EvidenceBody(issue, attribution.Attempt, kind, head)
	if err != nil || issue != attribution.Issue {
		if err == nil {
			err = errors.New("evidence issue does not match mutation attribution")
		}
		return err
	}
	return a.CreateIssueComment(ctx, repository, issue, body, attribution)
}

func (a API) EnsureEvidence(ctx context.Context, repository string, issue, attempt int, head string, actorID int) error {
	if actorID <= 0 {
		return errors.New("evidence publication requires the coordinator actor")
	}
	attribution := Mutation{Issue: issue, Attempt: attempt}
	for _, kind := range []string{"validation", "documentation"} {
		body, err := EvidenceBody(issue, attempt, kind, head)
		if err != nil {
			return err
		}
		present, err := HasAttemptComment(ctx, a, repository, issue, body, actorID)
		if err != nil {
			return err
		}
		if !present {
			if err := a.PublishEvidence(ctx, repository, issue, kind, head, attribution); err != nil {
				return err
			}
		}
	}
	return nil
}

var regexpSHA = regexp.MustCompile(`^[0-9a-fA-F]{7,64}$`)

func (a API) PublishPolicyStatus(ctx context.Context, repository, head string, result PolicyResult, attribution Mutation) error {
	if repository == "" || head == "" {
		return errors.New("policy status repository and head SHA are required")
	}
	summary := "Policy satisfied."
	if len(result.MergeBlockers) > 0 {
		summary = strings.Join(result.MergeBlockers, "; ")
	}
	description := fmt.Sprintf("issue #%d attempt %d: %s", attribution.Issue, attribution.Attempt, summary)
	if len(description) > 140 {
		description = description[:137] + "..."
	}
	body := map[string]string{"context": PolicyCheck, "state": policyStatus(result), "description": description}
	return a.mutateAttributed(ctx, http.MethodPost, fmt.Sprintf("/repos/%s/statuses/%s", repository, head), body, attribution)
}

func policyStatus(result PolicyResult) string {
	if result.CheckStatus != "completed" {
		return "pending"
	}
	if result.CheckConclusion == "success" {
		return "success"
	}
	return "failure"
}

func (a API) MergePullRequest(ctx context.Context, repository string, number int, expectedHead, method string, attribution Mutation) error {
	if expectedHead == "" || !slices.Contains([]string{"merge", "squash", "rebase"}, method) {
		return errors.New("merge requires an expected head SHA and an allowed method")
	}
	var result struct {
		Merged *bool `json:"merged"`
	}
	if err := a.mutateAttributedResult(ctx, http.MethodPut, fmt.Sprintf("/repos/%s/pulls/%d/merge", repository, number), map[string]string{"sha": expectedHead, "merge_method": method}, attribution, &result); err != nil {
		return err
	}
	if result.Merged == nil {
		return &ambiguousMutationError{errors.New("GitHub merge response omitted merged state")}
	}
	if !*result.Merged {
		return errors.New("GitHub declined pull request merge")
	}
	return nil
}

// PullRequestMerged resolves an uncertain merge only from GitHub's dedicated
// merge-status endpoint: 204 means merged and 404 means definitively unmerged.
func (a API) PullRequestMerged(ctx context.Context, repository string, number int) (bool, error) {
	resp, err := a.do(ctx, http.MethodGet, fmt.Sprintf("/repos/%s/pulls/%d/merge", repository, number), "", nil, Mutation{})
	if err != nil {
		return false, fmt.Errorf("GitHub pull request merge status is unknown: %w", err)
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusNoContent:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	default:
		return false, responseError("GitHub pull request merge status is unknown", resp)
	}
}

func (a API) mutateAttributed(ctx context.Context, method, path string, body any, attribution Mutation) error {
	return a.mutateAttributedResult(ctx, method, path, body, attribution, nil)
}

func (a API) mutateAttributedResult(ctx context.Context, method, path string, body any, attribution Mutation, dst any) error {
	if attribution.Issue <= 0 || attribution.Attempt <= 0 {
		return errors.New("GitHub mutation requires issue and attempt attribution")
	}
	var b []byte
	if body != nil {
		var err error
		b, err = json.Marshal(body)
		if err != nil {
			return err
		}
	}
	resp, err := a.do(ctx, method, path, "", b, attribution)
	if err != nil {
		return &ambiguousMutationError{fmt.Errorf("GitHub mutation outcome is ambiguous; reconcile issue #%d attempt %d: %w", attribution.Issue, attribution.Attempt, err)}
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return responseError("GitHub mutation", resp)
	}
	if dst != nil {
		if err := decodeJSON(resp.Body, dst); err != nil {
			return &ambiguousMutationError{fmt.Errorf("GitHub mutation outcome is ambiguous; reconcile issue #%d attempt %d: decode response: %w", attribution.Issue, attribution.Attempt, err)}
		}
	}
	return nil
}
