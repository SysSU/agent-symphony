package github

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

type IssueInput struct {
	Number, AuthorID    int
	NodeID, State, Body string
	CreatedAt           time.Time
	Labels              []string
	Cancelled, Retry    bool
}

type ContractConfig struct {
	Ready, P1, P2, P3 string
	DependencySection string
	RequiredSections  []string
	DefaultCompletion string
	HumanReview       string
	AutonomousMerge   string
}

type Controls struct {
	Ready        bool   `json:"ready"`
	Priority     int    `json:"priority"`
	Dependencies []int  `json:"dependencies"`
	Completion   string `json:"completion"`
	Closed       bool   `json:"closed"`
	Cancelled    bool   `json:"cancelled"`
	Retry        bool   `json:"retry"`
}

type NormalizedIssue struct {
	Number   int
	Controls Controls
	Ready    bool
	Blockers []string
}

var issueReference = regexp.MustCompile(`(?m)(?:^|\s)#([1-9][0-9]*)(?:\s|$|[,.;])`)

func NormalizeIssue(issue IssueInput, cfg ContractConfig, completed map[int]bool) NormalizedIssue {
	result := NormalizedIssue{Number: issue.Number}
	result.Controls.Closed = issue.State != "open"
	result.Controls.Cancelled = issue.Cancelled
	result.Controls.Retry = issue.Retry
	if issue.State != "open" {
		result.Blockers = append(result.Blockers, "issue is closed")
	}
	if issue.Cancelled {
		result.Blockers = append(result.Blockers, "issue is cancelled")
	}
	result.Controls.Ready = slices.Contains(issue.Labels, cfg.Ready)
	if !result.Controls.Ready {
		result.Blockers = append(result.Blockers, "readiness label is missing")
	}
	required := cfg.RequiredSections
	if len(required) == 0 {
		required = []string{"Context", "Acceptance Criteria", "Tasks", "Validation"}
	}
	for _, name := range required {
		section, ok := markdownSection(issue.Body, name)
		if !ok || strings.TrimSpace(section) == "" {
			result.Blockers = append(result.Blockers, "required "+strings.ToLower(name)+" section is missing or empty")
		}
	}
	for priority, label := range map[int]string{1: cfg.P1, 2: cfg.P2, 3: cfg.P3} {
		if slices.Contains(issue.Labels, label) {
			if result.Controls.Priority != 0 {
				result.Blockers = append(result.Blockers, "exactly one priority label is required")
			}
			result.Controls.Priority = priority
		}
	}
	if result.Controls.Priority == 0 {
		result.Blockers = append(result.Blockers, "exactly one priority label is required")
	}
	section, ok := markdownSection(issue.Body, cfg.DependencySection)
	if !ok {
		result.Blockers = append(result.Blockers, "required dependencies section is missing")
	} else {
		for _, match := range issueReference.FindAllStringSubmatch(section, -1) {
			n, _ := strconv.Atoi(match[1])
			if n == issue.Number {
				result.Blockers = append(result.Blockers, "issue depends on itself")
			} else if !slices.Contains(result.Controls.Dependencies, n) {
				result.Controls.Dependencies = append(result.Controls.Dependencies, n)
			}
		}
		slices.Sort(result.Controls.Dependencies)
		for _, dependency := range result.Controls.Dependencies {
			if !completed[dependency] {
				result.Blockers = append(result.Blockers, fmt.Sprintf("dependency #%d is incomplete", dependency))
			}
		}
	}
	result.Controls.Completion = cfg.DefaultCompletion
	if result.Controls.Completion == "" {
		result.Controls.Completion = "human-review"
	}
	human, autonomous := slices.Contains(issue.Labels, cfg.HumanReview), slices.Contains(issue.Labels, cfg.AutonomousMerge)
	if human && autonomous {
		result.Blockers = append(result.Blockers, "completion policy labels conflict")
	} else if human {
		result.Controls.Completion = "human-review"
	} else if autonomous {
		result.Controls.Completion = "autonomous-merge"
	}
	result.Ready = len(result.Blockers) == 0
	return result
}

type PermissionAuthorizer interface {
	Permission(actorID int) (string, error)
}

func AuthorizedControlActor(actorID, appID int, authorizer PermissionAuthorizer) (bool, error) {
	if actorID <= 0 || actorID == appID || authorizer == nil {
		return false, nil
	}
	permission, err := authorizer.Permission(actorID)
	if err != nil {
		return false, err
	}
	return permission == "maintain" || permission == "admin", nil
}

func markdownSection(body, name string) (string, bool) {
	lines := strings.Split(body, "\n")
	start := -1
	for i, line := range lines {
		if strings.EqualFold(strings.TrimSpace(strings.TrimPrefix(line, "##")), name) && strings.HasPrefix(strings.TrimSpace(line), "##") {
			start = i + 1
			continue
		}
		if start >= 0 && strings.HasPrefix(strings.TrimSpace(line), "##") {
			return strings.Join(lines[start:i], "\n"), true
		}
	}
	if start >= 0 {
		return strings.Join(lines[start:], "\n"), true
	}
	return "", false
}

type Anchor struct {
	EditID      string    `json:"edit_id,omitempty"`
	IssueNodeID string    `json:"issue_node_id,omitempty"`
	CreatedAt   time.Time `json:"created_at,omitempty"`
	ChangedAt   time.Time `json:"changed_at"`
	AuthorID    int       `json:"author_id,omitempty"`
}

type Provenance struct {
	Name, Value string
	Source      string
	EventID     int64
	ActorID     int
	CreatedAt   time.Time
}

type Approval struct {
	CommentID int64
	ActorID   int
	Body      string
	CreatedAt time.Time
}

type Snapshot struct {
	Version       int          `json:"version"`
	ControlsHash  string       `json:"controls_hash"`
	BodyHash      string       `json:"body_hash"`
	Anchor        Anchor       `json:"anchor"`
	ApprovalID    int64        `json:"approval_id"`
	ApprovalActor int          `json:"approval_actor"`
	Provenance    []Provenance `json:"provenance"`
}

type TimelineVerifier func(Provenance) bool

func NewSnapshot(controls Controls, body string, anchor Anchor, approval Approval, provenance []Provenance, command string, authorized func(int) bool, timeline TimelineVerifier) (Snapshot, error) {
	if authorized == nil || timeline == nil {
		return Snapshot{}, errors.New("actor authorizer and authoritative timeline verifier are required")
	}
	if !anchor.valid() || approval.CommentID <= 0 || approval.Body != command || !approval.CreatedAt.After(anchor.ChangedAt) || approval.ActorID == 0 || !authorized(approval.ActorID) {
		return Snapshot{}, errors.New("approval command is missing, stale, edited, or unauthorized")
	}
	required := map[string]string{
		"ready":      strconv.FormatBool(controls.Ready),
		"priority":   strconv.Itoa(controls.Priority),
		"completion": controls.Completion,
		"closed":     strconv.FormatBool(controls.Closed),
		"cancelled":  strconv.FormatBool(controls.Cancelled),
		"retry":      strconv.FormatBool(controls.Retry),
	}
	seen := make(map[string]bool, len(required))
	for _, p := range provenance {
		creationDefault := p.Value == map[string]string{"ready": "false", "priority": "0", "completion": "human-review", "closed": "false", "cancelled": "false", "retry": "false"}[p.Name]
		creation := p.Source == "creation" && creationDefault && p.EventID == 0 && p.ActorID == anchor.AuthorID && p.CreatedAt.Equal(anchor.CreatedAt)
		mutationSource := p.Source == "timeline" && slices.Contains([]string{"ready", "priority", "completion", "closed"}, p.Name) || p.Source == "comment" && slices.Contains([]string{"cancelled", "retry"}, p.Name)
		mutation := mutationSource && p.ActorID != 0 && !p.CreatedAt.IsZero() && authorized(p.ActorID) && timeline(p)
		if !creation && !mutation {
			return Snapshot{}, errors.New("control provenance is missing or unauthorized")
		}
		value, ok := required[p.Name]
		if !ok || seen[p.Name] || p.Value != value {
			return Snapshot{}, errors.New("control provenance is extra, duplicate, conflicting, or mismatched")
		}
		seen[p.Name] = true
	}
	if len(seen) != len(required) {
		return Snapshot{}, errors.New("current non-body control provenance is missing")
	}
	canonical := slices.Clone(provenance)
	slices.SortFunc(canonical, func(a, b Provenance) int { return strings.Compare(a.Name, b.Name) })
	return Snapshot{Version: snapshotVersion, ControlsHash: hashJSON(controls), BodyHash: bodyHash(body, anchor), Anchor: anchor, ApprovalID: approval.CommentID, ApprovalActor: approval.ActorID, Provenance: canonical}, nil
}

func (s Snapshot) Valid(controls Controls, body string, anchor Anchor, approval Approval, provenance []Provenance, command string, authorized func(int) bool, timeline TimelineVerifier) bool {
	want, err := NewSnapshot(controls, body, anchor, approval, provenance, command, authorized, timeline)
	return err == nil && hashJSON(s) == hashJSON(want)
}

func (a Anchor) valid() bool {
	return !a.ChangedAt.IsZero() && (a.EditID != "" || a.IssueNodeID != "" && !a.CreatedAt.IsZero() && a.AuthorID > 0)
}

const (
	snapshotVersion = 2
	snapshotPrefix  = "<!-- agent-symphony:controls:v2\n"
)

func SnapshotComment(snapshot Snapshot) string {
	b, _ := json.Marshal(snapshot)
	return snapshotPrefix + string(b) + "\n-->"
}

func ParseSnapshotComment(body string, authorID, appID int) (Snapshot, error) {
	if authorID != appID || !strings.HasPrefix(body, snapshotPrefix) || len(body) > 16<<10 || !strings.HasSuffix(body, "\n-->") {
		return Snapshot{}, errors.New("invalid or non-App control snapshot")
	}
	var snapshot Snapshot
	dec := json.NewDecoder(strings.NewReader(strings.TrimSuffix(strings.TrimPrefix(body, snapshotPrefix), "\n-->")))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&snapshot); err != nil || snapshot.Version != snapshotVersion {
		return Snapshot{}, errors.New("invalid control snapshot schema")
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return Snapshot{}, errors.New("invalid control snapshot trailing data")
	}
	return snapshot, nil
}

func AttributedBody(issue, attempt int, message string) (string, error) {
	if issue <= 0 || attempt <= 0 || strings.TrimSpace(message) == "" {
		return "", errors.New("issue update requires issue, attempt, and message")
	}
	return fmt.Sprintf("%s\n\n<!-- agent-symphony:issue:%d:attempt:%d -->", message, issue, attempt), nil
}

func AttemptMarker(issue, attempt int, branch, head string, pr int, outcome string) (string, error) {
	want, err := AttemptBranchFromBranch(branch, issue, attempt)
	if err != nil || branch != want || !regexpSHA.MatchString(head) || pr <= 0 || outcome != "review" {
		return "", errors.New("attempt marker requires its deterministic branch, head, PR, and review outcome")
	}
	b, _ := json.Marshal(recoveryMarkerPayload{Version: 1, Issue: issue, Attempt: attempt, Branch: branch, Head: head, PR: pr, Outcome: outcome})
	return recoveryMarkerPrefix + string(b) + "\n-->", nil
}
func hashJSON(v any) string {
	b, _ := json.Marshal(v)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
func bodyHash(body string, anchor Anchor) string {
	return hashJSON(struct {
		Body   string `json:"body"`
		Anchor Anchor `json:"anchor"`
	}{body, anchor})
}
