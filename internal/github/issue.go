package github

import (
	"bytes"
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

	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/ast"
	goldmarktext "github.com/yuin/goldmark/text"
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
	IssueFilter       string
	DependencySection string
	DefaultCompletion string
	HumanReview       string
	AutonomousMerge   string
}

type Controls struct {
	Ready        bool   `json:"ready"`
	IssueFilter  bool   `json:"issue_filter,omitempty"`
	Priority     int    `json:"priority"`
	Dependencies []int  `json:"dependencies"`
	Completion   string `json:"completion"`
	Closed       bool   `json:"closed"`
	Cancelled    bool   `json:"cancelled"`
	Retry        bool   `json:"retry"`
}

type NormalizedIssue struct {
	Number           int
	Controls         Controls
	Ready            bool
	Blockers         []string
	contractBlockers []string
}

var issueReference = regexp.MustCompile(`(?m)(?:^|\s)#([1-9][0-9]*)(?:\s|$|[,.;])`)
var checklistItem = regexp.MustCompile(`(?m)^[ \t]*(?:[-*+]|[0-9]{1,9}[.)])[ \t]+\[[ xX]\][ \t]+\S`)

func NormalizeIssue(issue IssueInput, cfg ContractConfig, completed map[int]bool) NormalizedIssue {
	result := NormalizedIssue{Number: issue.Number}
	result.contractBlockers = issueContractBlockers(issue.Body, cfg.DependencySection)
	result.Blockers = append(result.Blockers, result.contractBlockers...)
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
	if cfg.IssueFilter != "" {
		result.Controls.IssueFilter = slices.Contains(issue.Labels, cfg.IssueFilter)
		if !result.Controls.IssueFilter {
			result.Blockers = append(result.Blockers, "issue filter label is missing")
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
	if section, ok := markdownSection(issue.Body, cfg.DependencySection); ok {
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

func issueContractBlockers(body, dependencySection string) []string {
	if dependencySection == "" {
		dependencySection = "Dependencies"
	}
	sections := []string{"Context", "Acceptance criteria", "Checklist", "Validation", dependencySection}
	var blockers []string
	for _, name := range sections {
		section, ok := markdownSection(body, name)
		if !ok {
			blockers = append(blockers, fmt.Sprintf("required ## %s section is missing", name))
			continue
		}
		if strings.TrimSpace(section) == "" {
			blockers = append(blockers, fmt.Sprintf("required ## %s section is empty", name))
			continue
		}
		if name == "Checklist" && !checklistItem.MatchString(section) {
			blockers = append(blockers, "## Checklist must contain a Markdown task")
		}
		if name == dependencySection && !dependenciesDeclared(section) {
			blockers = append(blockers, fmt.Sprintf("## %s must contain issue references or None", name))
		}
	}
	return blockers
}

func dependenciesDeclared(section string) bool {
	if issueReference.MatchString(section) {
		return true
	}
	for _, line := range strings.Split(section, "\n") {
		value := strings.TrimSpace(line)
		value = strings.TrimSpace(strings.TrimLeft(value, "-*+"))
		if strings.EqualFold(strings.TrimSuffix(value, "."), "none") {
			return true
		}
	}
	return false
}

type PermissionAuthorizer interface {
	Permission(actorID int) (string, error)
}

func AuthorizedControlActor(actorID int, authorizer PermissionAuthorizer) (bool, error) {
	if actorID <= 0 || authorizer == nil {
		return false, nil
	}
	permission, err := authorizer.Permission(actorID)
	if err != nil {
		return false, err
	}
	return permission == "maintain" || permission == "admin", nil
}

func markdownSection(body, name string) (string, bool) {
	source := []byte(body)
	document := goldmark.DefaultParser().Parse(goldmarktext.NewReader(source))
	start, end := -1, len(source)
	for node := document.FirstChild(); node != nil; node = node.NextSibling() {
		heading, ok := node.(*ast.Heading)
		if !ok {
			continue
		}
		segment := heading.Lines().At(0)
		lineStart := markdownLineStart(source, segment.Start)
		if start < 0 {
			prefix := bytes.TrimSpace(source[lineStart:segment.Start])
			if heading.Level == 2 && bytes.Equal(prefix, []byte("##")) && strings.EqualFold(string(heading.Text(source)), name) {
				start = markdownLineEnd(source, segment.Stop)
			}
			continue
		}
		if heading.Level <= 2 {
			end = lineStart
			if end > start {
				end--
			}
			break
		}
	}
	if start < 0 {
		return "", false
	}
	section := append([]byte(nil), source[start:end]...)
	_ = ast.Walk(document, func(node ast.Node, entering bool) (ast.WalkStatus, error) {
		if !entering {
			return ast.WalkContinue, nil
		}
		switch node.Kind() {
		case ast.KindCodeBlock, ast.KindFencedCodeBlock:
			markdownMaskLines(section, start, node.Lines())
			return ast.WalkSkipChildren, nil
		case ast.KindHTMLBlock:
			block := node.(*ast.HTMLBlock)
			markdownMaskLines(section, start, block.Lines())
			if block.HasClosure() {
				markdownMask(section, start, block.ClosureLine)
			}
			return ast.WalkSkipChildren, nil
		case ast.KindRawHTML:
			raw := node.(*ast.RawHTML)
			for i := 0; i < raw.Segments.Len(); i++ {
				markdownMask(section, start, raw.Segments.At(i))
			}
		}
		return ast.WalkContinue, nil
	})
	return string(section), true
}

func markdownLineStart(source []byte, offset int) int {
	return bytes.LastIndexByte(source[:offset], '\n') + 1
}

func markdownLineEnd(source []byte, offset int) int {
	if newline := bytes.IndexByte(source[offset:], '\n'); newline >= 0 {
		return offset + newline + 1
	}
	return len(source)
}

func markdownMaskLines(section []byte, offset int, lines *goldmarktext.Segments) {
	for i := 0; i < lines.Len(); i++ {
		markdownMask(section, offset, lines.At(i))
	}
}

func markdownMask(section []byte, offset int, segment goldmarktext.Segment) {
	for i := max(segment.Start, offset); i < min(segment.Stop, offset+len(section)); i++ {
		if section[i-offset] != '\n' && section[i-offset] != '\r' {
			section[i-offset] = ' '
		}
	}
}

func IssuePaths(body string) []string {
	section, ok := markdownSection(body, "Paths")
	if !ok {
		return nil
	}
	var paths []string
	for _, line := range strings.Split(section, "\n") {
		path := strings.TrimSpace(line)
		path = strings.TrimSpace(strings.TrimPrefix(path, "-"))
		path = strings.Trim(path, "`")
		if path != "" {
			paths = append(paths, path)
		}
	}
	slices.Sort(paths)
	return slices.Compact(paths)
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
	labelOnly := approval == (Approval{})
	if !anchor.valid() || !labelOnly && (approval.CommentID <= 0 || approval.Body != command || !approval.CreatedAt.After(anchor.ChangedAt) || approval.ActorID == 0 || !authorized(approval.ActorID)) {
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
	if controls.IssueFilter || slices.ContainsFunc(provenance, func(p Provenance) bool { return p.Name == "issue_filter" }) {
		required["issue_filter"] = strconv.FormatBool(controls.IssueFilter)
	}
	seen := make(map[string]bool, len(required))
	var ready, autonomous Provenance
	for _, p := range provenance {
		creationDefault := p.Value == map[string]string{"ready": "false", "issue_filter": "false", "priority": "0", "completion": "human-review", "closed": "false", "cancelled": "false", "retry": "false"}[p.Name]
		creation := p.Source == "creation" && creationDefault && p.EventID == 0 && p.ActorID == anchor.AuthorID && p.CreatedAt.Equal(anchor.CreatedAt)
		mutationSource := p.Source == "timeline" && slices.Contains([]string{"ready", "issue_filter", "priority", "completion", "closed"}, p.Name) || p.Source == "comment" && slices.Contains([]string{"cancelled", "retry"}, p.Name)
		mutation := mutationSource && p.ActorID != 0 && !p.CreatedAt.IsZero() && (labelOnly || approval.CreatedAt.After(p.CreatedAt)) && authorized(p.ActorID) && timeline(p)
		if !creation && !mutation {
			return Snapshot{}, errors.New("control provenance is missing or unauthorized")
		}
		if p.Name == "ready" {
			ready = p
		} else if p.Name == "completion" {
			autonomous = p
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
	if labelOnly {
		if ready.Source != "timeline" || ready.Value != "true" || ready.CreatedAt.Before(anchor.ChangedAt) || ready.CreatedAt.Equal(anchor.ChangedAt) && anchor.EditID != "" {
			return Snapshot{}, errors.New("ready label does not authorize the current issue body")
		}
		if controls.Completion == "autonomous-merge" && (autonomous.Source != "timeline" || autonomous.Value != "autonomous-merge" || autonomous.CreatedAt.Before(anchor.ChangedAt) || autonomous.CreatedAt.Equal(anchor.ChangedAt) && anchor.EditID != "") {
			return Snapshot{}, errors.New("autonomous label does not authorize the current issue body")
		}
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

func ParseSnapshotComment(body string, authorID, coordinatorID int) (Snapshot, error) {
	if authorID != coordinatorID || !strings.HasPrefix(body, snapshotPrefix) || len(body) > 16<<10 || !strings.HasSuffix(body, "\n-->") {
		return Snapshot{}, errors.New("invalid or non-coordinator control snapshot")
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
