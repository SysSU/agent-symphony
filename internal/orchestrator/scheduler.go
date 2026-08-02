// Package orchestrator contains pure backlog policy. Runtime effects belong elsewhere.
package orchestrator

import (
	"cmp"
	"fmt"
	"path"
	"slices"
	"strings"
	"time"
)

type Issue struct {
	Repository   string
	Number       int
	Priority     int
	CreatedAt    time.Time
	Dependencies []int
	Paths        []string
	Eligible     bool
	Blockers     []string
	Active       bool
	Completed    bool
	Cancelled    bool
}

type Capacity struct {
	Global       int
	Repositories map[string]int
}

type State string

const (
	Blocked   State = "blocked"
	Queued    State = "queued"
	Runnable  State = "runnable"
	Active    State = "active"
	Completed State = "completed"
	Cancelled State = "cancelled"
)

type Decision struct {
	Repository  string
	Number      int
	State       State
	Explanation string
}

// Schedule recomputes a deterministic projection from authoritative current facts.
// It retains no delivery, cancellation, or queue state between calls.
func Schedule(input []Issue, capacity Capacity) []Decision {
	issues, contradictions := normalize(input)
	byRepo := make(map[string]map[int]Issue)
	for _, issue := range issues {
		if contradictions[key(issue)] {
			continue
		}
		if byRepo[issue.Repository] == nil {
			byRepo[issue.Repository] = make(map[int]Issue)
		}
		byRepo[issue.Repository][issue.Number] = issue
	}
	cycles := dependencyCycles(byRepo)

	slices.SortFunc(issues, compareIssues)
	decisions := make([]Decision, 0, len(issues))
	globalActive := 0
	repositoryActive := make(map[string]int)
	locks := make([]Issue, 0)
	for _, issue := range issues {
		if !contradictions[key(issue)] && issue.Active && !issue.Cancelled && !issue.Completed {
			globalActive++
			repositoryActive[issue.Repository]++
			locks = append(locks, issue)
		}
	}

	for _, issue := range issues {
		decision := Decision{Repository: issue.Repository, Number: issue.Number}
		switch {
		case contradictions[key(issue)]:
			decision.State, decision.Explanation = Blocked, "blocked: contradictory snapshots for this issue"
		case issue.Completed:
			decision.State, decision.Explanation = Completed, "completed: GitHub reports completed work"
		case issue.Cancelled:
			decision.State, decision.Explanation = Cancelled, "cancelled: current GitHub state cancels the issue"
		case issue.Active:
			decision.State, decision.Explanation = Active, "active: an existing attempt already owns capacity"
		case !issue.Eligible || len(issue.Blockers) > 0:
			decision.State, decision.Explanation = Blocked, "blocked: "+explainBlockers(issue)
		case issue.Number <= 0 || issue.Repository == "" || issue.Priority < 1 || issue.Priority > 3 || issue.CreatedAt.IsZero():
			decision.State, decision.Explanation = Blocked, "blocked: invalid normalized issue identity, priority, or creation time"
		case slices.Contains(issue.Dependencies, issue.Number):
			decision.State, decision.Explanation = Blocked, "blocked: issue depends on itself"
		case cycles[key(issue)]:
			decision.State, decision.Explanation = Blocked, "blocked: dependency cycle"
		default:
			if reason := dependencyBlocker(issue, byRepo[issue.Repository]); reason != "" {
				decision.State, decision.Explanation = Blocked, reason
			} else if capacity.Global <= 0 || globalActive >= capacity.Global {
				decision.State, decision.Explanation = Queued, "queued: global capacity is full"
			} else if limit := repositoryLimit(issue.Repository, capacity); limit <= 0 || repositoryActive[issue.Repository] >= limit {
				decision.State, decision.Explanation = Queued, "queued: repository capacity is full"
			} else if conflict := scopeConflict(issue, locks); conflict != nil {
				decision.State, decision.Explanation = Queued, fmt.Sprintf("queued: path scope is not provably disjoint from #%d", conflict.Number)
			} else {
				decision.State, decision.Explanation = Runnable, "runnable: dependencies, path scope, and capacity permit dispatch"
				globalActive++
				repositoryActive[issue.Repository]++
				locks = append(locks, issue)
			}
		}
		decisions = append(decisions, decision)
	}
	return decisions
}

func normalize(input []Issue) ([]Issue, map[string]bool) {
	seen := make(map[string]Issue)
	contradictions := make(map[string]bool)
	for _, issue := range input {
		issue.Dependencies = sortedUnique(issue.Dependencies)
		issue.Paths = sortedUniqueStrings(issue.Paths)
		issue.Blockers = sortedUniqueStrings(issue.Blockers)
		k := key(issue)
		if previous, ok := seen[k]; ok && !equalIssue(previous, issue) {
			contradictions[k] = true
			if compareIssues(issue, previous) < 0 {
				seen[k] = issue
			}
			continue
		}
		seen[k] = issue
	}
	issues := make([]Issue, 0, len(seen))
	for _, issue := range seen {
		issues = append(issues, issue)
	}
	return issues, contradictions
}

func equalIssue(a, b Issue) bool {
	return a.Repository == b.Repository && a.Number == b.Number && a.Priority == b.Priority && a.CreatedAt.Equal(b.CreatedAt) &&
		slices.Equal(a.Dependencies, b.Dependencies) && slices.Equal(a.Paths, b.Paths) && a.Eligible == b.Eligible &&
		slices.Equal(a.Blockers, b.Blockers) && a.Active == b.Active && a.Completed == b.Completed && a.Cancelled == b.Cancelled
}

func compareIssues(a, b Issue) int {
	if n := cmp.Compare(a.Priority, b.Priority); n != 0 {
		return n
	}
	if n := a.CreatedAt.Compare(b.CreatedAt); n != 0 {
		return n
	}
	if n := cmp.Compare(a.Number, b.Number); n != 0 {
		return n
	}
	return strings.Compare(a.Repository, b.Repository)
}

func dependencyCycles(repositories map[string]map[int]Issue) map[string]bool {
	cycles := make(map[string]bool)
	for repository, issues := range repositories {
		state := make(map[int]uint8)
		var stack []int
		var visit func(int)
		visit = func(number int) {
			state[number] = 1
			stack = append(stack, number)
			for _, dependency := range issues[number].Dependencies {
				if _, known := issues[dependency]; !known {
					continue
				}
				if state[dependency] == 0 {
					visit(dependency)
				} else if state[dependency] == 1 {
					for i := len(stack) - 1; i >= 0; i-- {
						cycles[fmt.Sprintf("%s#%d", repository, stack[i])] = true
						if stack[i] == dependency {
							break
						}
					}
				}
			}
			stack = stack[:len(stack)-1]
			state[number] = 2
		}
		for number := range issues {
			if state[number] == 0 {
				visit(number)
			}
		}
	}
	return cycles
}

func dependencyBlocker(issue Issue, known map[int]Issue) string {
	for _, dependency := range issue.Dependencies {
		if dependency == issue.Number {
			return "blocked: issue depends on itself"
		}
		other, ok := known[dependency]
		if !ok {
			return fmt.Sprintf("blocked: dependency #%d is unknown", dependency)
		}
		if !other.Completed {
			return fmt.Sprintf("blocked: dependency #%d is incomplete", dependency)
		}
	}
	return ""
}

func scopeConflict(issue Issue, locks []Issue) *Issue {
	for i := range locks {
		if issue.Repository != locks[i].Repository {
			continue
		}
		if !validScope(issue.Paths) || !validScope(locks[i].Paths) || pathsOverlap(issue.Paths, locks[i].Paths) {
			return &locks[i]
		}
	}
	return nil
}

func validScope(paths []string) bool {
	if len(paths) == 0 {
		return false
	}
	for _, p := range paths {
		if p == "" || p == "." || p == ".." || path.IsAbs(p) || strings.ContainsAny(p, "\\\x00") || path.Clean(p) != p || strings.HasPrefix(p, "../") {
			return false
		}
	}
	return true
}

func pathsOverlap(a, b []string) bool {
	for _, left := range a {
		for _, right := range b {
			if left == right || strings.HasPrefix(left, right+"/") || strings.HasPrefix(right, left+"/") {
				return true
			}
		}
	}
	return false
}

func repositoryLimit(repository string, capacity Capacity) int {
	if limit, ok := capacity.Repositories[repository]; ok {
		return limit
	}
	return capacity.Global
}

func explainBlockers(issue Issue) string {
	if len(issue.Blockers) == 0 {
		return "issue is ineligible"
	}
	return strings.Join(issue.Blockers, "; ")
}

func key(issue Issue) string { return fmt.Sprintf("%s#%d", issue.Repository, issue.Number) }

func sortedUnique(values []int) []int {
	values = slices.Clone(values)
	slices.Sort(values)
	return slices.Compact(values)
}

func sortedUniqueStrings(values []string) []string {
	values = slices.Clone(values)
	slices.Sort(values)
	return slices.Compact(values)
}
