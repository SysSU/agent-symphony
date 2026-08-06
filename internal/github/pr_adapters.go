package github

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"
)

var attemptMarker = regexp.MustCompile(`agent-symphony:issue:(\d+):attempt:(\d+)`)
var mergeMarker = regexp.MustCompile(`(?m)^Merge attempt for head \x60([0-9a-fA-F]{7,64})\x60: \*\*(prepared|dispatched|resolved)\*\*\.$`)
var feedbackDispositionMarker = regexp.MustCompile(`^Feedback (conversation|inline|review):([1-9][0-9]*): \*\*(addressed|blocked)\*\*\n\n([^\x00]*)\n\n<!-- agent-symphony:issue:([1-9][0-9]*):attempt:([1-9][0-9]*) -->$`)

const (
	feedbackConversation = "conversation"
	feedbackInline       = "inline"
	feedbackReview       = "review"
)

type requiredCheck struct {
	Context string `json:"context"`
	AppID   int64  `json:"app_id"`
}

// PRAdapterConfig contains repository policy names already owned by repository config.
type PRAdapterConfig struct {
	Repository, ReadyLabel, HumanReviewLabel, AutonomousMergeLabel, MergeMethod string
	PriorityP1Label, PriorityP2Label, PriorityP3Label, DependencySection        string
	DefaultCompletion, ApprovalCommand, CancelCommand, RetryCommand             string
	AppID                                                                       int64
	AppActorID                                                                  int
}

// FileRecovery is the durable handoff from reconciliation to issue recovery.
type FileRecovery struct{ Path string }

type RecoveryHandoff struct {
	Key                  string     `json:"key"`
	Repository           string     `json:"repository"`
	PR                   int        `json:"pr"`
	Issue                int        `json:"issue"`
	Attempt              int        `json:"attempt"`
	HeadSHA              string     `json:"head_sha"`
	Validation           bool       `json:"validation,omitempty"`
	ValidationGeneration uint64     `json:"validation_generation,omitempty"`
	Feedback             []Feedback `json:"feedback,omitempty"`
}

type FeedbackOutcome struct {
	ID       int64         `json:"id"`
	Source   string        `json:"source"`
	State    FeedbackState `json:"state"`
	Evidence string        `json:"evidence"`
}
type HandoffOutcome struct {
	Key                string            `json:"key"`
	Feedback           []FeedbackOutcome `json:"feedback,omitempty"`
	ValidationResult   string            `json:"validation_result,omitempty"`
	ValidationEvidence string            `json:"validation_evidence,omitempty"`
	Retryable          bool              `json:"retryable,omitempty"`
}

func handoffKey(h RecoveryHandoff) string {
	identities := make([]string, len(h.Feedback))
	for i, feedback := range h.Feedback {
		identities[i] = fmt.Sprintf("%s:%d", feedback.Source, feedback.ID)
	}
	slices.Sort(identities)
	return fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf("%s\x00%d\x00%d\x00%d\x00%s\x00%d\x00%s", h.Repository, h.PR, h.Issue, h.Attempt, h.HeadSHA, h.ValidationGeneration, strings.Join(identities, ",")))))
}

// ClaimHandoffs is the isolated runtime boundary for issue #4. Claims are
// persisted before work is returned, so restart or duplicate reconciliation
// cannot execute the same feedback twice.
func (r *FileRecovery) ClaimHandoffs(ctx context.Context) ([]RecoveryHandoff, error) {
	return r.ClaimHandoffsFor(ctx, nil)
}

// ClaimHandoffsFor atomically claims only attempts whose verified owner key is
// present. A nil owner set retains the legacy all-owner adapter behavior.
func (r *FileRecovery) ClaimHandoffsFor(_ context.Context, owners map[string]bool) ([]RecoveryHandoff, error) {
	var handoffs []RecoveryHandoff
	err := r.update(func(states []PRState) error {
		for i := range states {
			state := &states[i]
			if owners != nil && !owners[fmt.Sprintf("%s#%d/%d", state.Repository, state.Issue, state.Attempt)] {
				continue
			}
			validation := state.ValidationQueuedSHA == state.HeadSHA && state.HeadSHA != ""
			if validation {
				state.ValidationQueuedSHA, state.ValidationInFlightSHA = "", state.HeadSHA
			}
			validation = validation || state.ValidationInFlightSHA == state.HeadSHA && state.HeadSHA != ""
			h := RecoveryHandoff{Repository: state.Repository, PR: state.Number, Issue: state.Issue, Attempt: state.Attempt, HeadSHA: state.HeadSHA, Validation: validation, ValidationGeneration: state.ValidationGeneration}
			for j := range state.Facts.Feedback {
				feedback := &state.Facts.Feedback[j]
				if feedback.Execution != FeedbackClaimed && feedback.Execution != FeedbackInFlight {
					continue
				}
				feedback.Execution = FeedbackInFlight
				h.Feedback = append(h.Feedback, *feedback)
			}
			h.Key = handoffKey(h)
			if state.HandoffReceipts[h.Key] {
				continue
			}
			if h.Validation || len(h.Feedback) > 0 {
				handoffs = append(handoffs, h)
			}
		}
		return nil
	})
	return handoffs, err
}

// ReceiptHandoff records recipient acceptance before processing. Replays are
// idempotent and ordinary reconciliation ticks no longer repaste the item.
func (r *FileRecovery) ReceiptHandoff(_ context.Context, handoff RecoveryHandoff) error {
	if handoff.Key == "" || handoff.Key != handoffKey(handoff) {
		return errors.New("handoff immutable key mismatch")
	}
	return r.update(func(states []PRState) error {
		for i := range states {
			state := &states[i]
			if state.Repository == handoff.Repository && state.Number == handoff.PR && state.Issue == handoff.Issue && state.Attempt == handoff.Attempt && state.HeadSHA == handoff.HeadSHA {
				if state.HandoffReceipts == nil {
					state.HandoffReceipts = map[string]bool{}
				}
				state.HandoffReceipts[handoff.Key] = true
				return nil
			}
		}
		return errors.New("recovered pull request attempt not found or head changed")
	})
}

func (r *FileRecovery) CompleteHandoff(_ context.Context, handoff RecoveryHandoff) error {
	return r.update(func(states []PRState) error {
		for i := range states {
			state := &states[i]
			if state.Repository != handoff.Repository || state.Number != handoff.PR || state.Issue != handoff.Issue || state.Attempt != handoff.Attempt || state.HeadSHA != handoff.HeadSHA {
				continue
			}
			if handoff.Validation && state.ValidationInFlightSHA == handoff.HeadSHA {
				state.ValidationInFlightSHA = ""
			}
			for _, done := range handoff.Feedback {
				for j := range state.Facts.Feedback {
					if state.Facts.Feedback[j].identity() == done.identity() && state.Facts.Feedback[j].Execution == FeedbackInFlight {
						state.Facts.Feedback[j].Execution = FeedbackCompleted
					}
				}
			}
			return nil
		}
		return errors.New("recovered pull request attempt not found or head changed")
	})
}

// CompleteHandoffOutcome records only explicit, evidenced outcomes. Missing or
// stale immutable keys fail closed and leave in-flight work retryable.
func (r *FileRecovery) CompleteHandoffOutcome(_ context.Context, handoff RecoveryHandoff, outcome HandoffOutcome) error {
	if handoff.Key == "" || outcome.Key != handoff.Key || handoff.Key != handoffKey(handoff) {
		return errors.New("handoff immutable key mismatch")
	}
	return r.update(func(states []PRState) error {
		for i := range states {
			state := &states[i]
			if state.Repository != handoff.Repository || state.Number != handoff.PR || state.Issue != handoff.Issue || state.Attempt != handoff.Attempt || state.HeadSHA != handoff.HeadSHA {
				continue
			}
			if handoff.Validation {
				if outcome.ValidationEvidence == "" || (outcome.ValidationResult != "passed" && outcome.ValidationResult != "failed" && outcome.ValidationResult != "blocked") {
					return errors.New("validation outcome requires result and evidence")
				}
				if state.ValidationResult == outcome.ValidationResult && state.ValidationEvidence == outcome.ValidationEvidence && state.ValidationInFlightSHA == "" {
					// Identical replay after persist/before outcome-file removal.
				} else if state.ValidationInFlightSHA != handoff.HeadSHA {
					return errors.New("validation outcome does not match in-flight work")
				} else if !outcome.Retryable {
					state.ValidationInFlightSHA = ""
				}
				state.ValidationResult, state.ValidationEvidence = outcome.ValidationResult, outcome.ValidationEvidence
			}
			for _, got := range outcome.Feedback {
				if got.ID <= 0 || got.Evidence == "" || (got.State != FeedbackAddressed && got.State != FeedbackBlocked) {
					return errors.New("feedback outcome requires addressed/blocked state and evidence")
				}
				matched := false
				for j := range state.Facts.Feedback {
					f := &state.Facts.Feedback[j]
					if f.ID == got.ID && f.Source == got.Source && f.Execution == FeedbackCompleted && f.State == got.State && f.Evidence == got.Evidence {
						matched = true
					} else if f.ID == got.ID && f.Source == got.Source && f.Execution == FeedbackInFlight {
						f.State, f.Execution, f.Evidence = got.State, FeedbackCompleted, got.Evidence
						matched = true
					}
				}
				if !matched {
					return errors.New("feedback outcome does not match in-flight work")
				}
			}
			return nil
		}
		return errors.New("recovered pull request attempt not found or head changed")
	})
}

func (r *FileRecovery) PullRequestState(_ context.Context, repository string, number, issue, attempt int, head string) (PRState, error) {
	states, err := r.read()
	if err != nil {
		return PRState{}, err
	}
	for _, state := range states {
		if state.Repository == repository && state.Number == number && state.Issue == issue && state.Attempt == attempt {
			state.Facts.BranchModifiedOutsideAttempt = state.Facts.BranchModifiedOutsideAttempt || state.HeadSHA == "" || state.HeadSHA != head
			return state, nil
		}
	}
	return PRState{}, errors.New("recovered pull request attempt not found")
}
func (r *FileRecovery) ClaimFeedback(_ context.Context, state PRState, feedback Feedback) (bool, error) {
	claimed := false
	err := r.update(func(states []PRState) error {
		for i := range states {
			if !sameAttempt(states[i], state) {
				continue
			}
			if state.HeadSHA == "" || states[i].HeadSHA != state.HeadSHA {
				return errors.New("feedback claim head no longer matches recovery state")
			}
			for j := range states[i].Facts.Feedback {
				if states[i].Facts.Feedback[j].identity() != feedback.identity() {
					continue
				}
				f := &states[i].Facts.Feedback[j]
				if f.Execution != "" || f.Delegated || f.State == FeedbackAddressed || f.State == FeedbackBlocked {
					return nil
				}
				f.State, f.Execution, f.Delegated = FeedbackPending, FeedbackClaimed, true
				claimed = true
				return nil
			}
			feedback.State, feedback.Execution, feedback.Delegated = FeedbackPending, FeedbackClaimed, true
			states[i].Facts.Feedback = append(states[i].Facts.Feedback, feedback)
			claimed = true
			return nil
		}
		return errors.New("recovered pull request attempt not found")
	})
	return claimed, err
}

func (r *FileRecovery) QueueValidation(_ context.Context, state PRState) error {
	return r.update(func(states []PRState) error {
		for i := range states {
			if sameAttempt(states[i], state) {
				if state.HeadSHA == "" {
					return errors.New("validation head SHA is required")
				}
				states[i].ValidationQueuedSHA = state.HeadSHA
				states[i].ValidationGeneration++
				return nil
			}
		}
		return errors.New("recovered pull request attempt not found")
	})
}

func sameAttempt(a, b PRState) bool {
	return a.Repository == b.Repository && a.Number == b.Number && a.Issue == b.Issue && a.Attempt == b.Attempt
}

func (r *FileRecovery) update(change func([]PRState) error) error {
	lock, err := openRegular(r.Path+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	defer func() { _ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN) }()
	states, err := r.read()
	if err != nil {
		return err
	}
	if err := change(states); err != nil {
		return err
	}
	return r.write(states)
}
func (r *FileRecovery) read() ([]PRState, error) {
	f, err := openRegular(r.Path, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	b, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}
	var states []PRState
	if err := json.Unmarshal(b, &states); err != nil {
		return nil, err
	}
	return states, nil
}

func openRegular(path string, flag int, perm os.FileMode) (*os.File, error) {
	f, err := os.OpenFile(path, flag|syscall.O_NOFOLLOW, perm)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		f.Close()
		if err != nil {
			return nil, err
		}
		return nil, errors.New("recovery state and locks must be regular files")
	}
	return f, nil
}
func (r *FileRecovery) write(states []PRState) error {
	b, err := json.MarshalIndent(states, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(r.Path), ".pr-state-*")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if err = f.Chmod(0o600); err == nil {
		_, err = f.Write(append(b, '\n'))
	}
	if err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err := os.Rename(name, r.Path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(r.Path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

// RunPRReconciliation is the one-shot production bootstrap; issue #4 owns its daemon lifecycle.
func RunPRReconciliation(ctx context.Context, api API, cfg PRAdapterConfig, statePath string) error {
	recovery := &FileRecovery{Path: statePath}
	lock, err := openRegular(statePath+".governance.lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err := lock.Chmod(0o600); err != nil {
		return err
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	defer func() { _ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN) }()
	if err := api.VerifyInstallation(ctx, cfg.AppID, cfg.Repository); err != nil {
		return err
	}
	if _, err := os.Lstat(statePath); errors.Is(err, os.ErrNotExist) {
		if err := recovery.write([]PRState{}); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	source := &GitHubPRSource{API: api, Config: cfg, Recovery: recovery}
	reconciler, err := NewPRReconciler(api, cfg, recovery, func() error {
		states, err := recovery.read()
		if err != nil {
			return err
		}
		seen := map[int]bool{}
		for _, state := range states {
			if state.Repository != cfg.Repository || state.Issue <= 0 {
				return errors.New("recovery state contains an invalid issue identity")
			}
			if seen[state.Issue] {
				continue
			}
			seen[state.Issue] = true
			var facts PRFacts
			if err := source.readAuthorizedControls(ctx, state.Issue, &facts); err != nil {
				return fmt.Errorf("preflight issue %d: %w", state.Issue, err)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return reconciler.runOnce(ctx)
}

// AttemptRecovery is issue #4's durable attempt/recovery boundary.
type AttemptRecovery interface {
	PullRequestState(context.Context, string, int, int, int, string) (PRState, error)
	ClaimFeedback(context.Context, PRState, Feedback) (bool, error)
	QueueValidation(context.Context, PRState) error
}

// RecoverySignals is the production adapter from PR reconciliation to attempt recovery.
type RecoverySignals struct{ Recovery AttemptRecovery }

func (s RecoverySignals) DelegateFeedback(ctx context.Context, state PRState, feedback Feedback) error {
	claimed, err := s.Recovery.ClaimFeedback(ctx, state, feedback)
	if err == nil && !claimed {
		return nil
	}
	return err
}

func (s RecoverySignals) RerunValidation(ctx context.Context, state PRState) error {
	return s.Recovery.QueueValidation(ctx, state)
}

// GitHubPRSource reconstructs PR identity and policy facts from GitHub on every read.
// Attempt-local facts are joined through the durable issue #4 recovery owner.
type GitHubPRSource struct {
	API      API
	Config   PRAdapterConfig
	Recovery AttemptRecovery
}

func NewPRReconciler(api API, cfg PRAdapterConfig, recovery AttemptRecovery, fullRead func() error) (Reconciler, error) {
	if cfg.Repository == "" || cfg.ReadyLabel == "" || cfg.HumanReviewLabel == "" || cfg.AutonomousMergeLabel == "" || cfg.MergeMethod == "" || cfg.PriorityP1Label == "" || cfg.PriorityP2Label == "" || cfg.PriorityP3Label == "" || cfg.DependencySection == "" || cfg.DefaultCompletion == "" || cfg.ApprovalCommand == "" || cfg.CancelCommand == "" || cfg.RetryCommand == "" || cfg.CancelCommand == cfg.RetryCommand || cfg.AppID <= 0 || cfg.AppActorID <= 0 || recovery == nil || fullRead == nil {
		return Reconciler{}, errors.New("PR reconciliation requires repository policy, recovery, and issue reconciliation")
	}
	source := &GitHubPRSource{API: api, Config: cfg, Recovery: recovery}
	return Reconciler{FullRead: fullRead, PullRequests: &PRCoordinator{API: api, Source: source, Signals: RecoverySignals{recovery}, ReviewLabel: cfg.HumanReviewLabel, MergeMethod: cfg.MergeMethod}}, nil
}

func (s *GitHubPRSource) OpenPullRequests(ctx context.Context) ([]int, error) {
	var numbers []int
	for page := 1; ; page++ {
		var pulls []struct {
			Number int `json:"number"`
			Body   string
		}
		path := fmt.Sprintf("/repos/%s/pulls?state=open&per_page=100&page=%d", s.Config.Repository, page)
		if _, _, err := s.API.Read(ctx, path, "", &pulls); err != nil {
			return nil, err
		}
		for _, pull := range pulls {
			if pull.Number > 0 && exactAttemptMarker(pull.Body) {
				numbers = append(numbers, pull.Number)
			}
		}
		if len(pulls) < 100 {
			return numbers, nil
		}
	}
}

func (s *GitHubPRSource) FreshPullRequest(ctx context.Context, number int) (PRState, error) {
	var pull struct {
		Number           int `json:"number"`
		Body, State      string
		MergeableState   string `json:"mergeable_state"`
		Draft, Mergeable bool
		Merged           *bool
		Head             struct{ SHA string } `json:"head"`
		Base             struct{ Ref string } `json:"base"`
		Labels           []struct{ Name string }
	}
	if _, _, err := s.API.Read(ctx, fmt.Sprintf("/repos/%s/pulls/%d", s.Config.Repository, number), "", &pull); err != nil {
		return PRState{}, err
	}
	if pull.Merged == nil {
		return PRState{}, errors.New("pull request merged state is missing")
	}
	marker := attemptMarker.FindStringSubmatch(pull.Body)
	if len(marker) != 3 {
		return PRState{}, errors.New("pull request lacks issue/attempt marker")
	}
	var issue, attempt int
	if _, err := fmt.Sscanf(marker[0], "agent-symphony:issue:%d:attempt:%d", &issue, &attempt); err != nil || issue < 1 || attempt < 1 {
		return PRState{}, errors.New("pull request has invalid issue/attempt marker")
	}
	state, err := s.Recovery.PullRequestState(ctx, s.Config.Repository, number, issue, attempt, pull.Head.SHA)
	if err != nil {
		return PRState{}, err
	}
	state.Repository, state.Number, state.Issue, state.Attempt, state.HeadSHA = s.Config.Repository, number, issue, attempt, pull.Head.SHA
	state.Facts.HeadSHA, state.Facts.PRIsOpen, state.Facts.Draft, state.Facts.Mergeable = pull.Head.SHA, pull.State == "open" && !*pull.Merged, pull.Draft, pull.Mergeable
	state.Facts.Behind = pull.MergeableState == "behind"
	var issueData struct {
		State string
	}
	if _, _, err := s.API.Read(ctx, fmt.Sprintf("/repos/%s/issues/%d", s.Config.Repository, issue), "", &issueData); err != nil {
		return PRState{}, err
	}
	state.Facts.IssueOpen = issueData.State == "open"
	if err := s.readAuthorizedControls(ctx, issue, &state.Facts); err != nil {
		return PRState{}, err
	}
	if err := s.readIssueComments(ctx, issue, attempt, &state); err != nil {
		return PRState{}, err
	}
	recoveredFeedback := slices.Clone(state.Facts.Feedback)
	state.ReviewLabelPresent = slices.ContainsFunc(pull.Labels, func(label struct{ Name string }) bool { return label.Name == s.Config.HumanReviewLabel })
	var protection struct {
		RequiredStatusChecks struct {
			Strict   bool
			Contexts []string
			Checks   []requiredCheck `json:"checks"`
		} `json:"required_status_checks"`
		RequiredPullRequestReviews *struct {
			DismissStale bool `json:"dismiss_stale_reviews"`
			CodeOwners   bool `json:"require_code_owner_reviews"`
			Count        int  `json:"required_approving_review_count"`
			LastPush     bool `json:"require_last_push_approval"`
		} `json:"required_pull_request_reviews"`
	}
	if _, _, err := s.API.Read(ctx, "/repos/"+s.Config.Repository+"/branches/"+url.PathEscape(pull.Base.Ref)+"/protection", "", &protection); err != nil && !isResponseStatus(err, http.StatusNotFound) {
		return PRState{}, err
	}
	required := protection.RequiredStatusChecks.Checks
	for _, context := range protection.RequiredStatusChecks.Contexts {
		required = append(required, requiredCheck{Context: context})
	}
	state.Facts.BaseRequiresCurrent = protection.RequiredStatusChecks.Strict
	state.Facts.BranchProtectionAllows = true
	reviewCount, dismissStale := 0, false
	if protection.RequiredPullRequestReviews != nil {
		p := protection.RequiredPullRequestReviews
		state.Facts.CodeOwnerApprovalRequired, state.Facts.LastPushApprovalRequired = p.CodeOwners, p.LastPush
		reviewCount, dismissStale = p.Count, p.DismissStale
	}
	if err := s.readRules(ctx, pull.Base.Ref, &required, &reviewCount, &dismissStale, &state.Facts); err != nil {
		return PRState{}, err
	}
	state.Facts.ApprovalRequired = state.Facts.ApprovalRequired || reviewCount > 0 || state.Facts.CodeOwnerApprovalRequired || state.Facts.LastPushApprovalRequired
	if err := s.readApprovals(ctx, number, &state.Facts); err != nil {
		return PRState{}, err
	}
	state.Facts.PolicyCheckRequired = slices.ContainsFunc(required, func(r requiredCheck) bool {
		return r.Context == PolicyCheck && (r.AppID == 0 || r.AppID == s.Config.AppID)
	})
	if err := s.readReviews(ctx, number, &state.Facts); err != nil {
		return PRState{}, err
	}
	if err := s.readRequiredChecks(ctx, pull.Head.SHA, required, &state); err != nil {
		return PRState{}, err
	}
	var repository struct{ Permissions struct{ Push bool } }
	if _, _, err := s.API.Read(ctx, "/repos/"+s.Config.Repository, "", &repository); err != nil {
		return PRState{}, err
	}
	state.Facts.MergePermission = repository.Permissions.Push
	comments, err := s.readFeedback(ctx, number)
	if err != nil {
		return PRState{}, err
	}
	recovered := make(map[string]Feedback, len(recoveredFeedback))
	for _, f := range recoveredFeedback {
		recovered[f.identity()] = f
	}
	state.Facts.Feedback = make([]Feedback, 0, len(comments))
	for _, comment := range comments {
		if strings.TrimSpace(comment.Body) == "" || comment.User.ID == s.Config.AppActorID || comment.App != nil {
			continue
		}
		authorized, err := s.actorAuthorized(ctx, comment.User.ID)
		if err != nil {
			return PRState{}, err
		}
		fresh := Feedback{ID: comment.ID, Source: comment.Source, ActorID: comment.User.ID, Body: comment.Body, CreatedAt: comment.CreatedAt, Authorized: authorized}
		if old, ok := recovered[fresh.identity()]; ok {
			fresh.Execution, fresh.Delegated, fresh.Evidence = old.Execution, old.Delegated, old.Evidence
			if old.State == FeedbackAddressed || old.State == FeedbackBlocked {
				state.PendingDispositions = append(state.PendingDispositions, old)
			}
		}
		fresh.State = FeedbackPending
		if confirmed, ok := findFeedback(state.ConfirmedDispositions, fresh.identity()); ok {
			fresh.State, fresh.Execution = confirmed.State, ""
			state.PendingDispositions = slices.DeleteFunc(state.PendingDispositions, func(f Feedback) bool { return f.identity() == fresh.identity() })
		}
		state.Facts.Feedback = append(state.Facts.Feedback, fresh)
	}
	return state, nil
}

func findFeedback(feedback []Feedback, identity string) (Feedback, bool) {
	for _, f := range feedback {
		if f.identity() == identity {
			return f, true
		}
	}
	return Feedback{}, false
}

func (s *GitHubPRSource) readAuthorizedControls(ctx context.Context, issue int, facts *PRFacts) error {
	facts.IssueEligible, facts.NeedsHumanReview, facts.AutonomousMerge = false, true, false
	if s.Config.ApprovalCommand == "" {
		return nil
	}
	controls, humanReviewCleared, _, err := s.authorizedControls(ctx, issue)
	if err != nil {
		return fmt.Errorf("fresh authorized issue controls: %w", err)
	}
	facts.IssueEligible = facts.IssueOpen && controls.Ready
	facts.NeedsHumanReview = controls.Completion == "human-review" && !humanReviewCleared
	facts.AutonomousMerge = controls.Completion == "autonomous-merge"
	return nil
}

type issueControlRecord struct {
	Number    int
	NodeID    string `json:"node_id"`
	State     string
	Body      string
	CreatedAt time.Time `json:"created_at"`
	User      struct{ ID int }
	Labels    []struct{ Name string }
}

type issueTimelineEvent struct {
	ID        int64
	Event     string
	CreatedAt time.Time `json:"created_at"`
	Actor     struct{ ID int }
	Label     struct{ Name string }
}

type bodyEdit struct {
	ID       string
	EditedAt time.Time
	Editor   struct {
		Type       string `json:"__typename"`
		DatabaseID *int   `json:"databaseId"`
	}
}

func (s *GitHubPRSource) authorizedControls(ctx context.Context, number int) (Controls, bool, *Provenance, error) {
	return s.authorizedControlsWithIntake(ctx, number, false)
}

func (s *GitHubPRSource) authorizedControlsWithIntake(ctx context.Context, number int, intake bool) (Controls, bool, *Provenance, error) {
	var issue issueControlRecord
	if _, _, err := s.API.Read(ctx, fmt.Sprintf("/repos/%s/issues/%d", s.Config.Repository, number), "", &issue); err != nil {
		return Controls{}, false, nil, err
	}
	labels := make([]string, len(issue.Labels))
	for i := range issue.Labels {
		labels[i] = issue.Labels[i].Name
	}
	contract := ContractConfig{Ready: s.Config.ReadyLabel, P1: s.Config.PriorityP1Label, P2: s.Config.PriorityP2Label, P3: s.Config.PriorityP3Label, DependencySection: s.Config.DependencySection, DefaultCompletion: s.Config.DefaultCompletion, HumanReview: s.Config.HumanReviewLabel, AutonomousMerge: s.Config.AutonomousMergeLabel}
	input := IssueInput{Number: number, NodeID: issue.NodeID, State: issue.State, Body: issue.Body, CreatedAt: issue.CreatedAt, AuthorID: issue.User.ID, Labels: labels}
	events, err := s.issueTimeline(ctx, number)
	if err != nil {
		return Controls{}, false, nil, err
	}
	comments, err := s.issueComments(ctx, number)
	if err != nil {
		return Controls{}, false, nil, err
	}
	latestCommand, latestName := latestControlCommand(comments, s.Config.CancelCommand, s.Config.RetryCommand)
	if latestCommand != nil {
		input.Cancelled, input.Retry = latestName == "cancelled", latestName == "retry"
	}
	completed := map[int]bool{}
	if section, ok := markdownSection(issue.Body, contract.DependencySection); ok {
		for _, match := range issueReference.FindAllStringSubmatch(section, -1) {
			dependency, _ := strconv.Atoi(match[1])
			var current struct{ State string }
			if _, _, err := s.API.Read(ctx, fmt.Sprintf("/repos/%s/issues/%d", s.Config.Repository, dependency), "", &current); err != nil {
				return Controls{}, false, nil, err
			}
			completed[dependency] = current.State == "closed"
		}
	}
	normalized := NormalizeIssue(input, contract, completed)
	anchor := Anchor{IssueNodeID: issue.NodeID, CreatedAt: issue.CreatedAt, ChangedAt: issue.CreatedAt, AuthorID: issue.User.ID}
	anchorActor := 0
	edit, err := s.latestBodyEdit(ctx, number)
	if err != nil {
		return Controls{}, false, nil, err
	}
	if edit != nil {
		if edit.ID == "" || edit.Editor.DatabaseID == nil || *edit.Editor.DatabaseID <= 0 || edit.EditedAt.IsZero() {
			return Controls{}, false, nil, errors.New("issue body edit is ambiguous")
		}
		anchor = Anchor{EditID: edit.ID, ChangedAt: edit.EditedAt}
		anchorActor = *edit.Editor.DatabaseID
	}
	provenanceControls := normalized.Controls
	provenanceControls.Cancelled, provenanceControls.Retry = false, false
	currentProvenance, err := s.controlProvenance(issue, provenanceControls, events)
	if err != nil {
		return Controls{}, false, nil, err
	}
	if latestCommand != nil {
		applyControlCommandProvenance(currentProvenance, *latestCommand, latestName)
	}
	var snapshot Snapshot
	var snapshotCommentID int64
	found := false
	for _, comment := range comments {
		parsed, err := ParseSnapshotComment(comment.Body, comment.User.ID, s.Config.AppActorID)
		if err == nil && comment.App != nil && comment.App.ID == s.Config.AppID {
			if found && comment.ID == snapshotCommentID {
				return Controls{}, false, nil, errors.New("duplicate control snapshot")
			}
			if !found || comment.ID > snapshotCommentID {
				snapshot, snapshotCommentID, found = parsed, comment.ID, true
			}
		}
	}
	if !found && !intake {
		return Controls{}, false, nil, errors.New("valid App-authored control snapshot is missing")
	}
	provenance := currentProvenance
	approvalID := snapshot.ApprovalID
	if !found {
		if !normalized.Ready {
			return Controls{}, false, nil, errors.New("issue controls are not eligible for approval")
		}
		changedAt := anchor.ChangedAt
		for _, p := range provenance {
			if p.CreatedAt.After(changedAt) {
				changedAt = p.CreatedAt
			}
		}
		var latest *issueCommentRecord
		for i := range comments {
			comment := &comments[i]
			if comment.Body == s.Config.ApprovalCommand && comment.App == nil && comment.CreatedAt.Equal(comment.UpdatedAt) && comment.CreatedAt.After(changedAt) && (latest == nil || comment.CreatedAt.After(latest.CreatedAt) || comment.CreatedAt.Equal(latest.CreatedAt) && comment.ID > latest.ID) {
				latest = comment
			}
		}
		if latest == nil {
			return Controls{}, false, nil, errors.New("fresh exact approval command is missing")
		}
		approvalID = latest.ID
	}
	var approval struct {
		ID        int64
		Body      string
		CreatedAt time.Time `json:"created_at"`
		UpdatedAt time.Time `json:"updated_at"`
		User      struct{ ID int }
		App       *struct{ ID int64 } `json:"performed_via_github_app"`
	}
	if _, _, err := s.API.Read(ctx, fmt.Sprintf("/repos/%s/issues/comments/%d", s.Config.Repository, approvalID), "", &approval); err != nil {
		return Controls{}, false, nil, err
	}
	if approval.ID != approvalID || approval.Body != s.Config.ApprovalCommand || approval.App != nil || approval.CreatedAt.IsZero() || !approval.CreatedAt.Equal(approval.UpdatedAt) {
		return Controls{}, false, nil, errors.New("approval comment is missing, edited, or App-authored")
	}
	authorized := map[int]bool{}
	for _, actor := range append([]int{approval.User.ID, anchorActor}, provenanceActors(provenance)...) {
		if actor == 0 {
			continue
		}
		if _, ok := authorized[actor]; ok {
			continue
		}
		permission, err := s.actorPermission(ctx, actor)
		if err != nil {
			return Controls{}, false, nil, err
		}
		authorized[actor] = actor > 0 && actor != s.Config.AppActorID && (permission == "maintain" || permission == "admin")
	}
	timeline := make(map[Provenance]bool, len(provenance))
	for _, p := range provenance {
		timeline[p] = true
	}
	currentApproval := Approval{CommentID: approval.ID, ActorID: approval.User.ID, Body: approval.Body, CreatedAt: approval.CreatedAt}
	if !found {
		created, err := NewSnapshot(normalized.Controls, issue.Body, anchor, currentApproval, provenance, s.Config.ApprovalCommand, func(actor int) bool { return authorized[actor] }, func(p Provenance) bool { return timeline[p] })
		if err != nil {
			return Controls{}, false, nil, err
		}
		mutationErr := s.API.createControlSnapshot(ctx, s.Config.Repository, number, SnapshotComment(created))
		controls, reviewCleared, retry, readErr := s.authorizedControls(ctx, number)
		if readErr == nil {
			return controls, reviewCleared, retry, nil
		}
		if mutationErr != nil {
			return Controls{}, false, nil, mutationErr
		}
		return Controls{}, false, nil, fmt.Errorf("authoritative control snapshot reread: %w", readErr)
	}
	if !snapshot.Valid(normalized.Controls, issue.Body, anchor, currentApproval, provenance, s.Config.ApprovalCommand, func(actor int) bool { return authorized[actor] }, func(p Provenance) bool { return timeline[p] }) {
		return Controls{}, false, nil, errors.New("control snapshot is stale, tampered, unauthorized, or ambiguous")
	}
	var retry *Provenance
	if normalized.Controls.Retry {
		for i := range provenance {
			if provenance[i].Name == "retry" && provenance[i].Value == "true" {
				retry = &provenance[i]
			}
		}
	}
	return normalized.Controls, reviewRemovalMatches(provenance, events, s.Config.HumanReviewLabel), retry, nil
}

func reviewRemovalMatches(provenance []Provenance, events []issueTimelineEvent, label string) bool {
	for _, p := range provenance {
		if p.Name == "completion" && p.Source == "timeline" {
			return slices.ContainsFunc(events, func(e issueTimelineEvent) bool {
				return e.ID == p.EventID && e.Event == "unlabeled" && e.Label.Name == label
			})
		}
	}
	return false
}

func applyControlCommandProvenance(provenance []Provenance, command issueCommentRecord, winner string) {
	for i := range provenance {
		if provenance[i].Name == "cancelled" || provenance[i].Name == "retry" {
			provenance[i] = Provenance{Name: provenance[i].Name, Value: strconv.FormatBool(provenance[i].Name == winner), Source: "comment", EventID: command.ID, ActorID: command.User.ID, CreatedAt: command.CreatedAt}
		}
	}
}

func latestControlCommand(comments []issueCommentRecord, cancel, retry string) (*issueCommentRecord, string) {
	var latest *issueCommentRecord
	latestName := ""
	for i := range comments {
		c := &comments[i]
		name := map[string]string{cancel: "cancelled", retry: "retry"}[c.Body]
		if name != "" && c.App == nil && c.CreatedAt.Equal(c.UpdatedAt) && (latest == nil || c.CreatedAt.After(latest.CreatedAt) || c.CreatedAt.Equal(latest.CreatedAt) && c.ID > latest.ID) {
			latest, latestName = c, name
		}
	}
	return latest, latestName
}

func (s *GitHubPRSource) controlProvenance(issue issueControlRecord, controls Controls, events []issueTimelineEvent) ([]Provenance, error) {
	wants := map[string]string{"ready": strconv.FormatBool(controls.Ready), "priority": strconv.Itoa(controls.Priority), "completion": controls.Completion, "closed": strconv.FormatBool(controls.Closed), "cancelled": strconv.FormatBool(controls.Cancelled), "retry": strconv.FormatBool(controls.Retry)}
	labels := map[string]bool{}
	var latest = map[string]*issueTimelineEvent{}
	for i := range events {
		e := &events[i]
		name := ""
		switch {
		case e.Event == "closed" || e.Event == "reopened":
			name = "closed"
		case e.Event == "labeled" || e.Event == "unlabeled":
			labels[e.Label.Name] = e.Event == "labeled"
			if e.Label.Name == s.Config.ReadyLabel {
				name = "ready"
			}
			if slices.Contains([]string{s.Config.PriorityP1Label, s.Config.PriorityP2Label, s.Config.PriorityP3Label}, e.Label.Name) {
				name = "priority"
			}
			if slices.Contains([]string{s.Config.HumanReviewLabel, s.Config.AutonomousMergeLabel}, e.Label.Name) {
				name = "completion"
			}
		}
		if name != "" && (latest[name] == nil || e.CreatedAt.After(latest[name].CreatedAt) || e.CreatedAt.Equal(latest[name].CreatedAt) && e.ID > latest[name].ID) {
			latest[name] = e
		}
	}
	current := map[string]string{"ready": strconv.FormatBool(labels[s.Config.ReadyLabel]), "closed": strconv.FormatBool(issue.State == "closed"), "completion": s.Config.DefaultCompletion, "priority": "0", "cancelled": "false", "retry": "false"}
	for label, value := range map[string]string{s.Config.PriorityP1Label: "1", s.Config.PriorityP2Label: "2", s.Config.PriorityP3Label: "3"} {
		if labels[label] {
			if current["priority"] != "0" {
				return nil, errors.New("priority timeline is ambiguous")
			}
			current["priority"] = value
		}
	}
	for label, value := range map[string]string{s.Config.HumanReviewLabel: "human-review", s.Config.AutonomousMergeLabel: "autonomous-merge"} {
		if labels[label] {
			current["completion"] = value
		}
	}
	var result []Provenance
	for _, name := range []string{"ready", "priority", "completion", "closed", "cancelled", "retry"} {
		if current[name] != wants[name] {
			return nil, fmt.Errorf("current %s cannot be reconstructed from timeline", name)
		}
		e := latest[name]
		if e == nil {
			result = append(result, Provenance{Name: name, Value: wants[name], Source: "creation", ActorID: issue.User.ID, CreatedAt: issue.CreatedAt})
			continue
		}
		if e.ID <= 0 || e.Actor.ID <= 0 || e.CreatedAt.IsZero() {
			return nil, fmt.Errorf("current %s lacks exact timeline provenance", name)
		}
		result = append(result, Provenance{Name: name, Value: wants[name], Source: "timeline", EventID: e.ID, ActorID: e.Actor.ID, CreatedAt: e.CreatedAt})
	}
	return result, nil
}

func (s *GitHubPRSource) latestBodyEdit(ctx context.Context, number int) (*bodyEdit, error) {
	parts := strings.SplitN(s.Config.Repository, "/", 2)
	if len(parts) != 2 {
		return nil, errors.New("invalid repository")
	}
	payload := map[string]any{"query": `query($owner:String!,$repo:String!,$number:Int!){repository(owner:$owner,name:$repo){issue(number:$number){userContentEdits(last:1){nodes{id editedAt editor{__typename ... on Bot{databaseId} ... on Mannequin{databaseId} ... on Organization{databaseId} ... on User{databaseId}}}}}}}`, "variables": map[string]any{"owner": parts[0], "repo": parts[1], "number": number}}
	var response struct {
		Data struct {
			Repository struct {
				Issue struct {
					UserContentEdits struct{ Nodes []bodyEdit } `json:"userContentEdits"`
				}
			}
		}
		Errors []struct{ Message string }
	}
	if err := s.API.readGraphQL(ctx, payload, &response); err != nil {
		return nil, err
	}
	if len(response.Errors) > 0 {
		return nil, fmt.Errorf("GitHub GraphQL: %s", response.Errors[0].Message)
	}
	nodes := response.Data.Repository.Issue.UserContentEdits.Nodes
	if len(nodes) == 0 {
		return nil, nil
	}
	if len(nodes) != 1 {
		return nil, errors.New("issue body edit is ambiguous")
	}
	if !slices.Contains([]string{"Bot", "Mannequin", "Organization", "User"}, nodes[0].Editor.Type) || nodes[0].Editor.DatabaseID == nil || *nodes[0].Editor.DatabaseID <= 0 {
		return nil, errors.New("issue body editor identity is missing or unsupported")
	}
	return &nodes[0], nil
}

func provenanceActors(provenance []Provenance) []int {
	var actors []int
	for i := range provenance {
		if provenance[i].Source != "creation" {
			actors = append(actors, provenance[i].ActorID)
		}
	}
	return actors
}

type issueCommentRecord struct {
	ID        int64
	Body      string
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	User      struct{ ID int }
	App       *struct{ ID int64 } `json:"performed_via_github_app"`
}

func (s *GitHubPRSource) issueComments(ctx context.Context, number int) ([]issueCommentRecord, error) {
	var all []issueCommentRecord
	seen := map[int64]bool{}
	for page := 1; ; page++ {
		var batch []issueCommentRecord
		if _, _, err := s.API.Read(ctx, fmt.Sprintf("/repos/%s/issues/%d/comments?per_page=100&page=%d", s.Config.Repository, number, page), "", &batch); err != nil {
			return nil, err
		}
		for _, comment := range batch {
			if comment.ID <= 0 || seen[comment.ID] {
				return nil, errors.New("issue comments contain a missing or duplicate immutable ID")
			}
			seen[comment.ID] = true
			all = append(all, comment)
		}
		if len(batch) < 100 {
			return all, nil
		}
	}
}

func (s *GitHubPRSource) issueTimeline(ctx context.Context, number int) ([]issueTimelineEvent, error) {
	var all []issueTimelineEvent
	seen := map[int64]bool{}
	for page := 1; ; page++ {
		var batch []issueTimelineEvent
		if _, _, err := s.API.Read(ctx, fmt.Sprintf("/repos/%s/issues/%d/timeline?per_page=100&page=%d", s.Config.Repository, number, page), "", &batch); err != nil {
			return nil, err
		}
		for _, event := range batch {
			if !slices.Contains([]string{"labeled", "unlabeled", "closed", "reopened"}, event.Event) {
				continue
			}
			if event.ID <= 0 || seen[event.ID] {
				return nil, errors.New("issue timeline contains a missing or duplicate immutable ID")
			}
			seen[event.ID] = true
			all = append(all, event)
		}
		if len(batch) < 100 {
			return all, nil
		}
	}
}

func (s *GitHubPRSource) readReviewDecision(ctx context.Context, number int) (string, error) {
	owner, repository, ok := strings.Cut(s.Config.Repository, "/")
	if !ok {
		return "", errors.New("repository must be owner/name")
	}
	var body struct {
		Data struct {
			Repository struct {
				PullRequest *struct{ ReviewDecision *string }
			}
		}
		Errors []struct{ Message string }
	}
	payload := map[string]any{
		"query":     `query($owner:String!,$name:String!,$number:Int!){repository(owner:$owner,name:$name){pullRequest(number:$number){reviewDecision}}}`,
		"variables": map[string]any{"owner": owner, "name": repository, "number": number},
	}
	if err := s.API.readGraphQL(ctx, payload, &body); err != nil {
		return "", err
	}
	if len(body.Errors) > 0 {
		return "", fmt.Errorf("GitHub GraphQL review decision: %s", body.Errors[0].Message)
	}
	if body.Data.Repository.PullRequest == nil || body.Data.Repository.PullRequest.ReviewDecision == nil {
		return "", nil
	}
	return *body.Data.Repository.PullRequest.ReviewDecision, nil
}

func (s *GitHubPRSource) readApprovals(ctx context.Context, number int, facts *PRFacts) error {
	facts.Approved, facts.ChangesRequested, facts.CodeOwnerApproved, facts.LastPushApproved = false, false, false, false
	decision, err := s.readReviewDecision(ctx, number)
	if err != nil {
		return err
	}
	facts.Approved = decision == "APPROVED"
	facts.ChangesRequested = decision == "CHANGES_REQUESTED"
	facts.CodeOwnerApproved = facts.CodeOwnerApprovalRequired && facts.Approved
	facts.LastPushApproved = facts.LastPushApprovalRequired && facts.Approved
	return nil
}

type feedbackRecord struct {
	ID        int64
	Source    string
	Body      string
	CreatedAt time.Time `json:"created_at"`
	Submitted time.Time `json:"submitted_at"`
	User      struct{ ID int }
	App       *struct{ ID int64 } `json:"performed_via_github_app"`
}

func (s *GitHubPRSource) readFeedback(ctx context.Context, number int) ([]feedbackRecord, error) {
	var all []feedbackRecord
	paths := []struct{ path, source string }{
		{fmt.Sprintf("/repos/%s/issues/%d/comments", s.Config.Repository, number), feedbackConversation},
		{fmt.Sprintf("/repos/%s/pulls/%d/comments", s.Config.Repository, number), feedbackInline},
		{fmt.Sprintf("/repos/%s/pulls/%d/reviews", s.Config.Repository, number), feedbackReview},
	}
	for _, endpoint := range paths {
		for page := 1; ; page++ {
			var batch []feedbackRecord
			if _, _, err := s.API.Read(ctx, fmt.Sprintf("%s?per_page=100&page=%d", endpoint.path, page), "", &batch); err != nil {
				return nil, err
			}
			fullPage := len(batch) == 100
			for i := range batch {
				batch[i].Source = endpoint.source
				if batch[i].CreatedAt.IsZero() {
					batch[i].CreatedAt = batch[i].Submitted
				}
			}
			batch = slices.DeleteFunc(batch, func(f feedbackRecord) bool { return f.User.ID == s.Config.AppActorID || f.App != nil })
			all = append(all, batch...)
			if !fullPage {
				break
			}
		}
	}
	return all, nil
}

func (s *GitHubPRSource) readRules(ctx context.Context, branch string, required *[]requiredCheck, count *int, dismissStale *bool, facts *PRFacts) error {
	for page := 1; ; page++ {
		var rules []struct {
			Type       string
			Parameters struct {
				Checks []struct {
					Context       string
					IntegrationID int64 `json:"integration_id"`
				} `json:"required_status_checks"`
				Strict     bool `json:"strict_required_status_checks_policy"`
				Dismiss    bool `json:"dismiss_stale_reviews_on_push"`
				CodeOwners bool `json:"require_code_owner_review"`
				LastPush   bool `json:"require_last_push_approval"`
				Count      int  `json:"required_approving_review_count"`
			}
		}
		path := fmt.Sprintf("/repos/%s/rules/branches/%s?per_page=100&page=%d", s.Config.Repository, url.PathEscape(branch), page)
		if _, _, err := s.API.Read(ctx, path, "", &rules); err != nil {
			return err
		}
		for _, rule := range rules {
			switch rule.Type {
			case "required_status_checks":
				facts.BaseRequiresCurrent = facts.BaseRequiresCurrent || rule.Parameters.Strict
				for _, check := range rule.Parameters.Checks {
					*required = append(*required, requiredCheck{Context: check.Context, AppID: check.IntegrationID})
				}
			case "pull_request":
				*count, *dismissStale = max(*count, rule.Parameters.Count), *dismissStale || rule.Parameters.Dismiss
				facts.CodeOwnerApprovalRequired = facts.CodeOwnerApprovalRequired || rule.Parameters.CodeOwners
				facts.LastPushApprovalRequired = facts.LastPushApprovalRequired || rule.Parameters.LastPush
			case "merge_queue":
				facts.BranchProtectionAllows = false
			}
		}
		if len(rules) < 100 {
			return nil
		}
	}
}

func (s *GitHubPRSource) readIssueComments(ctx context.Context, issue, attempt int, state *PRState) error {
	marker := fmt.Sprintf("<!-- agent-symphony:issue:%d:attempt:%d -->", issue, attempt)
	state.MergeAttemptSHA = ""
	state.MergePhase = ""
	state.Facts.ValidationSHA, state.Facts.DocumentationSHA = "", ""
	type disposition struct {
		id    int64
		state string
	}
	latest := map[string]disposition{}
	evidenceIDs := map[string]int64{}
	feedbackDispositions := map[string]struct {
		id    int64
		state FeedbackState
	}{}
	decisionBodies := make(map[string]int, len(state.Decisions))
	for i, decision := range state.Decisions {
		body, err := AttributedBody(issue, attempt, fmt.Sprintf("Implementation decision %s\n\n%s", decision.ID, decision.Body))
		if err == nil {
			decisionBodies[body] = i
		}
	}
	for page := 1; ; page++ {
		var comments []struct {
			ID   int64
			Body string
			User struct{ ID int }
			App  *struct{ ID int64 } `json:"performed_via_github_app"`
		}
		path := fmt.Sprintf("/repos/%s/issues/%d/comments?per_page=100&page=%d", s.Config.Repository, issue, page)
		if _, _, err := s.API.Read(ctx, path, "", &comments); err != nil {
			return err
		}
		for _, comment := range comments {
			match := mergeMarker.FindStringSubmatch(comment.Body)
			attributed := strings.Contains(comment.Body, marker) && comment.App != nil && comment.App.ID == s.Config.AppID && comment.User.ID == s.Config.AppActorID && s.Config.AppID > 0 && s.Config.AppActorID > 0
			if len(match) == 3 && attributed {
				if old, ok := latest[match[1]]; !ok || comment.ID > old.id {
					latest[match[1]] = disposition{comment.ID, match[2]}
				}
			}
			if attributed {
				if match := feedbackDispositionMarker.FindStringSubmatch(comment.Body); len(match) == 7 {
					id, _ := strconv.ParseInt(match[2], 10, 64)
					feedback := Feedback{ID: id, Source: match[1], State: FeedbackState(match[3])}
					canonical, err := FeedbackDispositionBody(issue, attempt, feedback, match[4])
					if err == nil && comment.Body == canonical {
						old := feedbackDispositions[feedback.identity()]
						if comment.ID > old.id {
							feedbackDispositions[feedback.identity()] = struct {
								id    int64
								state FeedbackState
							}{comment.ID, feedback.State}
						}
					}
				}
				if i, ok := decisionBodies[comment.Body]; ok {
					state.Decisions[i].Recorded = true
				}
				for _, kind := range []string{"validation", "documentation"} {
					body, err := EvidenceBody(issue, attempt, kind, state.HeadSHA)
					if err == nil && comment.Body == body && comment.ID > evidenceIDs[kind] {
						evidenceIDs[kind] = comment.ID
						if kind == "validation" {
							state.Facts.ValidationSHA = state.HeadSHA
						} else {
							state.Facts.DocumentationSHA = state.HeadSHA
						}
					}
				}
			}
		}
		if len(comments) < 100 {
			for i := range state.Facts.Feedback {
				if disposition, ok := feedbackDispositions[state.Facts.Feedback[i].identity()]; ok {
					state.ConfirmedDispositions = append(state.ConfirmedDispositions, Feedback{ID: state.Facts.Feedback[i].ID, Source: state.Facts.Feedback[i].Source, State: disposition.state})
				}
			}
			if phase := latest[state.HeadSHA].state; phase == "prepared" || phase == "dispatched" {
				state.MergeAttemptSHA = state.HeadSHA
				state.MergePhase = phase
			}
			return nil
		}
	}
}

func (s *GitHubPRSource) readReviews(ctx context.Context, number int, facts *PRFacts) error {
	type review struct {
		ID        int64
		State     string
		CommitID  string    `json:"commit_id"`
		Submitted time.Time `json:"submitted_at"`
		User      struct{ ID int }
	}
	latest := map[int]review{}
	for page := 1; ; page++ {
		var reviews []review
		if _, _, err := s.API.Read(ctx, fmt.Sprintf("/repos/%s/pulls/%d/reviews?per_page=100&page=%d", s.Config.Repository, number, page), "", &reviews); err != nil {
			return err
		}
		for _, r := range reviews {
			if r.User.ID > 0 && slices.Contains([]string{"APPROVED", "CHANGES_REQUESTED", "DISMISSED"}, r.State) {
				old, ok := latest[r.User.ID]
				if !ok || r.Submitted.After(old.Submitted) || r.Submitted.Equal(old.Submitted) && r.ID > old.ID {
					latest[r.User.ID] = r
				}
			}
		}
		if len(reviews) < 100 {
			break
		}
	}
	for _, r := range latest {
		if r.State == "CHANGES_REQUESTED" {
			facts.ChangesRequested = true
		}
	}
	return nil
}

func (s *GitHubPRSource) readRequiredChecks(ctx context.Context, head string, required []requiredCheck, state *PRState) error {
	facts := &state.Facts
	type observed struct {
		ok      bool
		app, id int64
	}
	checks := map[string]observed{}
	for page := 1; ; page++ {
		var body struct {
			CheckRuns []struct {
				Name, Status, Conclusion string
				ID                       int64
				App                      struct{ ID int64 }
			} `json:"check_runs"`
		}
		if _, _, err := s.API.Read(ctx, fmt.Sprintf("/repos/%s/commits/%s/check-runs?filter=all&per_page=100&page=%d", s.Config.Repository, head, page), "", &body); err != nil {
			return err
		}
		for _, c := range body.CheckRuns {
			key := fmt.Sprintf("%s\x00%d", c.Name, c.App.ID)
			old, exists := checks[key]
			ok := c.Status == "completed" && slices.Contains([]string{"success", "neutral", "skipped"}, c.Conclusion)
			if !exists || c.ID > old.id {
				checks[key] = observed{ok, c.App.ID, c.ID}
			} else if c.ID == old.id {
				old.ok = old.ok && ok
				checks[key] = old
			}
		}
		if len(body.CheckRuns) < 100 {
			break
		}
	}
	if policy, ok := checks[fmt.Sprintf("%s\x00%d", PolicyCheck, s.Config.AppID)]; ok {
		state.CheckRunID, state.CheckHead = policy.id, head
	}
	statuses := map[string]bool{}
	for page := 1; ; page++ {
		var body []struct{ Context, State string }
		if _, _, err := s.API.Read(ctx, fmt.Sprintf("/repos/%s/commits/%s/statuses?per_page=100&page=%d", s.Config.Repository, head, page), "", &body); err != nil {
			return err
		}
		for _, status := range body {
			if _, seen := statuses[status.Context]; !seen {
				statuses[status.Context] = status.State == "success"
			}
		}
		if len(body) < 100 {
			break
		}
	}
	facts.RequiredChecksPass = true
	for _, want := range required {
		if want.Context == PolicyCheck {
			continue
		}
		var got observed
		checkOK := false
		for key, candidate := range checks {
			if !strings.HasPrefix(key, want.Context+"\x00") || want.AppID != 0 && candidate.app != want.AppID {
				continue
			}
			if !checkOK || candidate.id > got.id {
				got, checkOK = candidate, true
			}
		}
		ok := checkOK && got.ok
		if want.AppID == 0 && !checkOK {
			ok = statuses[want.Context]
		}
		if !ok {
			facts.RequiredChecksPass = false
		}
	}
	return nil
}

func (s *GitHubPRSource) FreshFeedback(ctx context.Context, state PRState, feedback Feedback) (Feedback, error) {
	var comment struct {
		ID        int64
		Body      string
		CreatedAt time.Time `json:"created_at"`
		User      struct{ ID int }
		App       *struct{ ID int64 } `json:"performed_via_github_app"`
	}
	var path string
	switch feedback.Source {
	case feedbackConversation:
		path = fmt.Sprintf("/repos/%s/issues/comments/%d", state.Repository, feedback.ID)
	case feedbackInline:
		path = fmt.Sprintf("/repos/%s/pulls/comments/%d", state.Repository, feedback.ID)
	case feedbackReview:
		path = fmt.Sprintf("/repos/%s/pulls/%d/reviews/%d", state.Repository, state.Number, feedback.ID)
	default:
		return Feedback{}, errors.New("unknown feedback source")
	}
	if _, _, err := s.API.Read(ctx, path, "", &comment); err != nil {
		return Feedback{}, err
	}
	if comment.User.ID == s.Config.AppActorID || comment.App != nil {
		return Feedback{ID: comment.ID, Source: feedback.Source, ActorID: comment.User.ID, Body: comment.Body, CreatedAt: comment.CreatedAt}, nil
	}
	authorized, err := s.actorAuthorized(ctx, comment.User.ID)
	return Feedback{ID: comment.ID, Source: feedback.Source, ActorID: comment.User.ID, Body: comment.Body, CreatedAt: comment.CreatedAt, Authorized: authorized}, err
}

func exactAttemptMarker(body string) bool {
	match := attemptMarker.FindAllStringSubmatch(body, -1)
	return len(match) == 1 && strings.Contains(body, "<!-- "+match[0][0]+" -->")
}

func (s *GitHubPRSource) actorAuthorized(ctx context.Context, actorID int) (bool, error) {
	permission, err := s.actorPermission(ctx, actorID)
	return strings.Contains(" admin maintain write ", " "+permission+" "), err
}

func (s *GitHubPRSource) actorPermission(ctx context.Context, actorID int) (string, error) {
	var user struct{ Login string }
	if _, _, err := s.API.Read(ctx, fmt.Sprintf("/user/%d", actorID), "", &user); err != nil {
		return "", err
	}
	var permission struct{ Permission string }
	path := "/repos/" + s.Config.Repository + "/collaborators/" + url.PathEscape(user.Login) + "/permission"
	if _, _, err := s.API.Read(ctx, path, "", &permission); err != nil {
		return "", err
	}
	return permission.Permission, nil
}
