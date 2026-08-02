package orchestrator

import (
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestScheduleBacklogBehavior(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	issue := func(repo string, number, priority int, age time.Duration, paths ...string) Issue {
		return Issue{Repository: repo, Number: number, Priority: priority, CreatedAt: now.Add(age), Paths: paths, Eligible: true}
	}

	var hundred []Issue
	for n := 100; n >= 1; n-- {
		hundred = append(hundred, issue("owner/repo", n, n%3+1, time.Duration(n)*time.Minute, "files/"+strconv.Itoa(n)))
	}
	got := Schedule(hundred, Capacity{Global: 100, Repositories: map[string]int{"owner/repo": 100}})
	if len(got) != 100 {
		t.Fatalf("100 issue projection has %d entries", len(got))
	}
	for i := 1; i < len(got); i++ {
		previous, current := hundredDecision(got[i-1], hundred), hundredDecision(got[i], hundred)
		if compareIssues(previous, current) > 0 || got[i].State != Runnable {
			t.Fatalf("non-deterministic ordering at %d: %#v %#v", i, got[i-1], got[i])
		}
	}

	base := []Issue{
		issue("a/repo", 1, 1, 0, "cmd/one"),
		issue("a/repo", 2, 1, 0, "cmd/two"), // issue number breaks the exact tie
		issue("a/repo", 3, 2, -time.Hour, "cmd/three"),
		issue("b/repo", 4, 1, time.Minute, "other"),
	}
	want := Schedule(base, Capacity{Global: 3, Repositories: map[string]int{"a/repo": 2, "b/repo": 2}})
	assertStates(t, want, map[string]State{"a/repo#1": Runnable, "a/repo#2": Runnable, "a/repo#3": Queued, "b/repo#4": Runnable})

	mixed := []Issue{
		{Repository: "a/repo", Number: 10, Completed: true},
		func() Issue { v := issue("a/repo", 11, 1, 0, "safe/a"); v.Dependencies = []int{10}; return v }(),
		func() Issue { v := issue("a/repo", 12, 1, 0, "safe/b"); v.Dependencies = []int{99}; return v }(),
		func() Issue { v := issue("a/repo", 13, 1, 0, "safe/c"); v.Dependencies = []int{13}; return v }(),
		func() Issue { v := issue("a/repo", 14, 1, 0, "cycle/a"); v.Dependencies = []int{15}; return v }(),
		func() Issue { v := issue("a/repo", 15, 1, 0, "cycle/b"); v.Dependencies = []int{14}; return v }(),
		func() Issue { v := issue("a/repo", 16, 1, 0, "active"); v.Active = true; return v }(),
		issue("a/repo", 17, 1, 0),                 // missing scope serializes
		issue("a/repo", 18, 1, 0, "../unsafe"),    // invalid scope serializes
		issue("a/repo", 19, 1, 0, "active/child"), // overlap serializes
		issue("a/repo", 20, 1, 0, "disjoint"),     // can run beside active
		func() Issue {
			v := issue("a/repo", 21, 1, 0, "cancelled")
			v.Cancelled = true
			v.Active = true
			return v
		}(),
	}
	result := Schedule(mixed, Capacity{Global: 3, Repositories: map[string]int{"a/repo": 3}})
	assertStates(t, result, map[string]State{
		"a/repo#10": Completed, "a/repo#11": Runnable, "a/repo#12": Blocked, "a/repo#13": Blocked,
		"a/repo#14": Blocked, "a/repo#15": Blocked, "a/repo#16": Active, "a/repo#17": Queued,
		"a/repo#18": Queued, "a/repo#19": Queued, "a/repo#20": Runnable, "a/repo#21": Cancelled,
	})

	duplicate := issue("a/repo", 30, 1, 0, "dup")
	contradiction := duplicate
	contradiction.Priority = 2
	result = Schedule([]Issue{duplicate, duplicate, contradiction}, Capacity{Global: 2})
	if len(result) != 1 || result[0].State != Blocked || !strings.Contains(result[0].Explanation, "contradictory") {
		t.Fatalf("duplicate/contradictory metadata: %#v", result)
	}
	if again := Schedule([]Issue{duplicate, duplicate}, Capacity{Global: 2}); !reflect.DeepEqual(again, Schedule([]Issue{duplicate}, Capacity{Global: 2})) {
		t.Fatalf("duplicate delivery changed projection: %#v", again)
	}

	cancelled := duplicate
	cancelled.Cancelled = true
	if got := Schedule([]Issue{cancelled}, Capacity{Global: 1}); got[0].State != Cancelled {
		t.Fatalf("cancellation not recomputed: %#v", got)
	}
	if got := Schedule([]Issue{duplicate}, Capacity{Global: 1}); got[0].State != Runnable {
		t.Fatalf("current state did not reevaluate independently: %#v", got)
	}
	unrelated := issue("a/repo", 31, 3, time.Hour, "unrelated")
	unrelated.Eligible, unrelated.Blockers = false, []string{"waiting for approval"}
	if got := Schedule([]Issue{duplicate, unrelated}, Capacity{Global: 1}); got[0].Number != duplicate.Number || got[0].State != Runnable {
		t.Fatalf("unrelated blocked progress stalled runnable work: %#v", got)
	}
	for _, decision := range result {
		if decision.Explanation == "" {
			t.Fatalf("missing explanation: %#v", decision)
		}
	}
}

func TestContradictorySnapshotsDoNotContributeFacts(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	issue := func(number int, path string) Issue {
		return Issue{Repository: "a/repo", Number: number, Priority: 1, CreatedAt: now.Add(time.Duration(number) * time.Minute), Paths: []string{path}, Eligible: true}
	}

	contradiction := issue(1, "shared")
	contradiction.Active = true
	other := contradiction
	other.Active, other.Completed = false, true
	other.Dependencies, other.Paths = []int{2}, []string{"other"}
	dependent := issue(2, "shared/child")
	dependent.Dependencies = []int{1}
	unrelated := issue(3, "shared/child")

	want := Schedule([]Issue{contradiction, other, dependent, unrelated}, Capacity{Global: 2})
	got := Schedule([]Issue{other, unrelated, dependent, contradiction}, Capacity{Global: 2})
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("permutation changed decisions:\n got: %#v\nwant: %#v", got, want)
	}
	assertStates(t, got, map[string]State{"a/repo#1": Blocked, "a/repo#2": Blocked, "a/repo#3": Runnable})
}

func TestSelfDependencyExplanationPrecedesCycle(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	issue := Issue{Repository: "a/repo", Number: 1, Priority: 1, CreatedAt: now, Dependencies: []int{1}, Paths: []string{"safe"}, Eligible: true}
	got := Schedule([]Issue{issue}, Capacity{Global: 1})
	if len(got) != 1 || got[0].Explanation != "blocked: issue depends on itself" {
		t.Fatalf("self dependency: %#v", got)
	}
}

func hundredDecision(decision Decision, issues []Issue) Issue {
	for _, issue := range issues {
		if issue.Repository == decision.Repository && issue.Number == decision.Number {
			return issue
		}
	}
	panic("decision missing input")
}

func assertStates(t *testing.T, decisions []Decision, want map[string]State) {
	t.Helper()
	for _, decision := range decisions {
		k := decision.Repository + "#" + strconv.Itoa(decision.Number)
		if want[k] != decision.State || decision.Explanation == "" {
			t.Errorf("%s = %s (%s), want %s", k, decision.State, decision.Explanation, want[k])
		}
		delete(want, k)
	}
	if len(want) != 0 {
		t.Errorf("missing decisions: %v", want)
	}
}
