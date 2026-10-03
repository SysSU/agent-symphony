package main

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

var testBootA = hostBootIdentity{Source: "linux-boot-id-v1", UUID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"}
var testBootB = hostBootIdentity{Source: "linux-boot-id-v1", UUID: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"}

func TestHostBootIdentityParsing(t *testing.T) {
	for _, source := range []string{"linux-boot-id-v1", "darwin-bootsessionuuid-v1"} {
		for _, value := range []string{testBootA.UUID, strings.ToUpper(testBootA.UUID)} {
			if got := parseHostBootIdentity(source, value); !got.valid() || got.UUID != testBootA.UUID {
				t.Fatalf("canonical identity %q: %#v", value, got)
			}
		}
		for _, value := range []string{"", "00000000-0000-0000-0000-000000000000", strings.Repeat("a", 36), testBootA.UUID + "\n", testBootA.UUID + "\x00", " " + testBootA.UUID, testBootA.UUID[:35], "z" + testBootA.UUID[1:], strings.Repeat("a", 1024)} {
			if got := parseHostBootIdentity(source, value); got != (hostBootIdentity{}) {
				t.Fatalf("accepted malformed identity %q: %#v", value, got)
			}
		}
	}
	if got := parseHostBootIdentity("unknown", testBootA.UUID); got.valid() {
		t.Fatal("accepted unknown identity source")
	}
}

func TestHostBootIdentityNative(t *testing.T) {
	if os.Getenv("AGENT_SYMPHONY_TEST_BOOT_READER") == "1" {
		body, _ := json.Marshal(readHostBootIdentity())
		_, _ = os.Stdout.Write(body)
		os.Exit(0)
	}
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("native boot identity is unavailable on this platform")
	}
	first := readHostBootIdentity()
	if !first.valid() {
		t.Fatal("supported native host has no valid boot identity")
	}
	for range 2 {
		cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestHostBootIdentityNative$")
		cmd.Env = append(os.Environ(), "AGENT_SYMPHONY_TEST_BOOT_READER=1")
		body, err := cmd.Output()
		var next hostBootIdentity
		if err != nil || json.Unmarshal(body, &next) != nil || next != first {
			t.Fatalf("identity changed across processes: first=%#v next=%#v err=%v", first, next, err)
		}
	}
}

func legacyBootFixture(t *testing.T, root string) runtimeOwnerState {
	t.Helper()
	if err := os.MkdirAll(runtimeOwnerAttemptRoot(root), 0o700); err != nil {
		t.Fatal(err)
	}
	state := newRuntimeOwnerState("o/r")
	state.Epoch, state.Revision = 1, 1
	issueKey, attemptKey := ownerIssueKey("o/r", 353), ownerAttemptKey("o/r", 353, 1)
	state.IssueGenerations[issueKey], state.AttemptGenerations[attemptKey] = 1, 2
	state.Tombstones[attemptKey] = runtimeTombstone{Repository: "o/r", Issue: 353, Attempt: 1, Action: "removed", Generation: 2, InvalidatedGeneration: 1, Revision: 1, CleanupPhase: "completed"}
	state.LegacyReviewerQuarantines[issueKey] = "legacy reviewer absence unknown; physical cleanup cannot be certified"
	return state
}

func restartLegacyBootOwner(t *testing.T, root string, initial runtimeOwnerState, boot hostBootIdentity) runtimeOwnerState {
	t.Helper()
	owner, err := startStateOwnerWithBootIdentity(t.Context(), root, runtimeOwnerAttemptRoot(root), initial, func(ctx context.Context, candidate runtimeOwnerState) error {
		return writeRuntimeOwnerStateContext(ctx, root, runtimeOwnerAttemptRoot(root), candidate)
	}, boot)
	if err != nil {
		t.Fatal(err)
	}
	state := mustOwnerSnapshot(t, owner).State
	if err := owner.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	loaded, err := readRuntimeOwnerState(root, "o/r")
	if err != nil || !maps.Equal(loaded.LegacyReviewerBaselines, state.LegacyReviewerBaselines) || !maps.Equal(loaded.LegacyReviewerReleases, state.LegacyReviewerReleases) || !maps.Equal(loaded.LegacyReviewerQuarantines, state.LegacyReviewerQuarantines) {
		t.Fatalf("restart lost committed boot transition: err=%v", err)
	}
	return state
}

func TestLegacyReviewerBootRecovery(t *testing.T) {
	root := resolvedTempDir(t)
	initial := legacyBootFixture(t, root)
	key := ownerIssueKey("o/r", 353)
	state := initial
	for _, test := range []struct {
		name    string
		boot    hostBootIdentity
		blocked bool
	}{
		{"unavailable", hostBootIdentity{}, true},
		{"baseline", testBootA, true},
		{"same boot", testBootA, true},
		{"identity temporarily unavailable", hostBootIdentity{}, true},
		{"verified reboot", testBootB, false},
		{"released restart", testBootB, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			state = restartLegacyBootOwner(t, root, state, test.boot)
			if got := issueHasUnprovedReviewer(state, "o/r", 353); got != test.blocked {
				t.Fatalf("dispatch quarantine=%t want=%t", got, test.blocked)
			}
			if !reflect.DeepEqual(state.Tombstones, initial.Tombstones) || !reflect.DeepEqual(state.AttemptGenerations, initial.AttemptGenerations) || !reflect.DeepEqual(state.ControlReceipts, initial.ControlReceipts) {
				t.Fatal("boot transition modified business history")
			}
			status, err := projectOwnerStatus(stateOwnerSnapshot{State: state}, 1, time.Unix(1, 0))
			if err != nil || len(status.Statuses) != map[bool]int{true: 1, false: 0}[test.blocked] {
				t.Fatalf("visible status=%#v err=%v", status, err)
			}
			if test.blocked && (status.Statuses[0].CurrentPhase != "physical-unverified" || !status.Statuses[0].OperatorBlocked || status.Statuses[0].DispatchAuthorized) {
				t.Fatal("quarantine did not block projected actions")
			}
			if test.blocked && !test.boot.valid() && !strings.Contains(status.Statuses[0].Diagnostic, "verified reboot recovery unavailable") {
				t.Fatal("unavailable identity has no actionable diagnostic")
			}
			if !test.blocked {
				candidate := cloneRuntimeOwnerState(state)
				if err := applyAdvanceIssueGeneration(&candidate, advanceIssueGenerationCommand{Repository: "o/r", Issue: 353, ExpectedGeneration: candidate.IssueGenerations[key]}); err != nil {
					t.Fatalf("legacy restriction still blocks generation advance: %v", err)
				}
			}
		})
	}
}

func TestLegacyReviewerBootNewEvidenceAndSourceChanges(t *testing.T) {
	for _, test := range []string{"new record", "new admission", "after release", "source changed", "invalid current identity"} {
		t.Run(test, func(t *testing.T) {
			root := resolvedTempDir(t)
			state := restartLegacyBootOwner(t, root, legacyBootFixture(t, root), testBootA)
			key := ownerIssueKey("o/r", 353)
			boot := testBootB
			switch test {
			case "new record":
				tombstone := state.Tombstones[ownerAttemptKey("o/r", 353, 1)]
				tombstone.Attempt = 2
				state.Tombstones[ownerAttemptKey("o/r", 353, 2)] = tombstone
				state.AttemptGenerations[ownerAttemptKey("o/r", 353, 2)] = 2
			case "after release":
				state = restartLegacyBootOwner(t, root, state, testBootB)
				fallthrough
			case "new admission":
				admitLegacyReviewerQuarantine(&state, key, "new legacy evidence")
			case "source changed":
				boot.Source = "darwin-bootsessionuuid-v1"
			case "invalid current identity":
				boot.UUID = "invalid"
			}
			state = restartLegacyBootOwner(t, root, state, boot)
			if state.LegacyReviewerQuarantines[key] == "" {
				t.Fatal("old evidence or incompatible identity authorized release")
			}
			if boot.valid() && state.LegacyReviewerBaselines[key].Boot != boot {
				t.Fatal("new evidence was not rebaselined")
			}
		})
	}
}

func TestLegacyReviewerBootPersistenceFailures(t *testing.T) {
	for _, release := range []bool{false, true} {
		for _, installed := range []bool{false, true} {
			t.Run(strings.Join([]string{map[bool]string{false: "baseline", true: "release"}[release], map[bool]string{false: "before install", true: "after install"}[installed]}, "/"), func(t *testing.T) {
				root := resolvedTempDir(t)
				initial := legacyBootFixture(t, root)
				if release {
					initial = restartLegacyBootOwner(t, root, initial, testBootA)
				} else if err := writeRuntimeOwnerState(root, runtimeOwnerAttemptRoot(root), initial); err != nil {
					t.Fatal(err)
				}
				before := cloneRuntimeOwnerState(initial)
				injected := errors.New("injected persistence failure")
				owner, err := startStateOwnerWithBootIdentity(t.Context(), root, runtimeOwnerAttemptRoot(root), initial, func(ctx context.Context, candidate runtimeOwnerState) error {
					if installed {
						if err := writeRuntimeOwnerStateContext(ctx, root, runtimeOwnerAttemptRoot(root), candidate); err != nil {
							return err
						}
						return statePersistenceInstalledError{err: injected}
					}
					return injected
				}, testBootB)
				if owner != nil || !errors.Is(err, injected) {
					t.Fatalf("failed startup admitted work: owner=%v err=%v", owner, err)
				}
				if !reflect.DeepEqual(initial, before) {
					t.Fatal("failed candidate mutated initial state")
				}
				loaded, err := readRuntimeOwnerState(root, "o/r")
				if err != nil {
					t.Fatal(err)
				}
				if installed {
					if (len(loaded.LegacyReviewerQuarantines) == 0) != release {
						t.Fatal("installed ledger was partially transitioned")
					}
				} else if !reflect.DeepEqual(loaded.LegacyReviewerBaselines, before.LegacyReviewerBaselines) || !reflect.DeepEqual(loaded.LegacyReviewerQuarantines, before.LegacyReviewerQuarantines) {
					t.Fatal("pre-install failure replaced the authoritative ledger")
				}
				converged := restartLegacyBootOwner(t, root, loaded, testBootB)
				if (len(converged.LegacyReviewerQuarantines) == 0) != release {
					t.Fatal("restart failed to converge")
				}
			})
		}
	}
}

func TestLegacyReviewerBootHistoricalManifestValidation(t *testing.T) {
	for _, tombstoned := range []bool{false, true} {
		t.Run(map[bool]string{false: "attempt", true: "tombstone"}[tombstoned], func(t *testing.T) {
			root := resolvedTempDir(t)
			state := legacyBootFixture(t, root)
			manifest := ownerTestManifest(t, root, 353, 1, "failed")
			manifest.ReviewState = "failed"
			manifest.ReviewDiagnostic = "historical reviewer failure"
			key := ownerAttemptKey("o/r", 353, 1)
			if tombstoned {
				tombstone := state.Tombstones[key]
				tombstone.Action = "dismissed"
				tombstone.Manifest = &manifest
				state.Tombstones[key] = tombstone
			} else {
				delete(state.Tombstones, key)
				manifest.State, manifest.Diagnostic = "running", ""
				state.Attempts[key] = runtimeAttemptRecord{Generation: 2, Manifest: manifest}
			}
			state = restartLegacyBootOwner(t, root, state, testBootA)
			state = restartLegacyBootOwner(t, root, state, testBootB)
			state = restartLegacyBootOwner(t, root, state, testBootB)
			if len(state.LegacyReviewerQuarantines) != 0 {
				t.Fatal("historical record recreated quarantine")
			}
			state.ReviewerSafetyMigrated, state.ReviewerRunTracked, state.ReviewerPolicyTracked = false, false, false
			state = restartLegacyBootOwner(t, root, state, testBootB)
			if len(state.LegacyReviewerQuarantines) != 0 {
				t.Fatal("later migration recreated released quarantine")
			}
			if !tombstoned {
				owner, err := startStateOwnerWithBootIdentity(t.Context(), root, runtimeOwnerAttemptRoot(root), state, func(ctx context.Context, candidate runtimeOwnerState) error {
					return writeRuntimeOwnerStateContext(ctx, root, runtimeOwnerAttemptRoot(root), candidate)
				}, testBootB)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = owner.close(context.Background()) })
				current := mustOwnerSnapshot(t, owner)
				_, _, err = owner.beginRuntimeEffect(t.Context(), beginRuntimeEffectCommand{Identity: stateResultIdentity{Epoch: current.State.Epoch, SourceRevision: current.State.Revision, IssueGeneration: 1, AttemptGeneration: 2}, Action: agentruntime.EffectStop, Manifest: manifest, Reason: "stop historical attempt", RequestDigest: strings.Repeat("f", 64)})
				if err != nil {
					t.Fatalf("released historical attempt could not enter Stop: %v", err)
				}
				_, _, err = owner.invalidateAttempt(t.Context(), invalidateAttemptCommand{Repository: "o/r", Issue: 353, Attempt: 1, ExpectedIssueGeneration: 1, ExpectedAttemptGeneration: 3, Action: "dismissed", CleanupPhase: "completed", Manifest: &manifest})
				if err != nil {
					t.Fatalf("released historical manifest could not move to a tombstone: %v", err)
				}
				if err := owner.close(t.Context()); err != nil {
					t.Fatal(err)
				}
				if _, err := readRuntimeOwnerState(root, "o/r"); err != nil {
					t.Fatal(err)
				}
			}
			newEvidence := cloneRuntimeOwnerState(state)
			newManifest := ownerTestManifest(t, root, 353, 2, "failed")
			newManifest.ReviewState, newManifest.ReviewDiagnostic = "failed", "new legacy reviewer evidence"
			newKey := ownerAttemptKey("o/r", 353, 2)
			newEvidence.AttemptGenerations[newKey] = 1
			newEvidence.Attempts[newKey] = runtimeAttemptRecord{Generation: 1, Manifest: newManifest}
			newEvidence.ReviewerRunTracked = false
			newEvidence = restartLegacyBootOwner(t, root, newEvidence, testBootA)
			if newEvidence.LegacyReviewerQuarantines[ownerIssueKey("o/r", 353)] == "" || newEvidence.LegacyReviewerBaselines[ownerIssueKey("o/r", 353)].Boot != testBootA {
				t.Fatal("new migration evidence reused an earlier release")
			}
			delete(state.Tombstones, key)
			state.AttemptGenerations[key]++
			state.Attempts[key] = runtimeAttemptRecord{Generation: state.AttemptGenerations[key], Manifest: manifest}
			if err := validateRuntimeOwnerState(state, runtimeOwnerAttemptRoot(root), root, true); err == nil {
				t.Fatal("historical release exempted a newer attempt generation")
			}
		})
	}
}

func TestLegacyReviewerBootRejectsMalformedProvenance(t *testing.T) {
	root := resolvedTempDir(t)
	state := restartLegacyBootOwner(t, root, legacyBootFixture(t, root), testBootA)
	key := ownerIssueKey("o/r", 353)
	for _, mutate := range []func(*runtimeOwnerState){
		func(s *runtimeOwnerState) {
			b := s.LegacyReviewerBaselines[key]
			b.Boot.UUID = "bad"
			s.LegacyReviewerBaselines[key] = b
		},
		func(s *runtimeOwnerState) {
			b := s.LegacyReviewerBaselines[key]
			b.Evidence = "bad"
			s.LegacyReviewerBaselines[key] = b
		},
		func(s *runtimeOwnerState) { delete(s.LegacyReviewerQuarantines, key) },
		func(s *runtimeOwnerState) {
			s.LegacyReviewerReleases = map[string]legacyReviewerRelease{strings.Repeat("a", 64): {Before: testBootA, After: testBootA}}
		},
	} {
		candidate := cloneRuntimeOwnerState(state)
		mutate(&candidate)
		body, err := json.Marshal(candidate)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, runtimeOwnerStateFile), body, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := readRuntimeOwnerState(root, "o/r"); err == nil {
			t.Fatal("malformed persisted provenance accepted")
		}
	}
}

func TestLegacyReviewerBootFreshUnavailable(t *testing.T) {
	root := resolvedTempDir(t)
	if err := os.MkdirAll(runtimeOwnerAttemptRoot(root), 0o700); err != nil {
		t.Fatal(err)
	}
	state := restartLegacyBootOwner(t, root, newRuntimeOwnerState("o/r"), hostBootIdentity{})
	if len(state.LegacyReviewerBaselines) != 0 || len(state.LegacyReviewerReleases) != 0 || len(state.LegacyReviewerQuarantines) != 0 {
		t.Fatal("fresh installation gained recovery state")
	}
	status, err := projectOwnerStatus(stateOwnerSnapshot{State: state}, 1, time.Now())
	if err != nil || len(status.Statuses) != 0 || status.ReconciliationError != "" {
		t.Fatalf("fresh unavailable projection=%#v err=%v", status, err)
	}
}

func TestLegacyReviewerBootPreservesIndependentBlockers(t *testing.T) {
	for _, kind := range []string{"stop", "completed superseded binding", "reviewer proof", "start candidate", "handoff candidate", "cleanup"} {
		t.Run(kind, func(t *testing.T) {
			root := resolvedTempDir(t)
			state := legacyBootFixture(t, root)
			key := ownerAttemptKey("o/r", 353, 1)
			manifest := ownerTestManifest(t, root, 353, 1, "failed")
			manifest.Version, manifest.LaunchToken, manifest.LaunchID = agentruntime.ManifestVersion2, strings.Repeat("a", 32), strings.Repeat("b", 32)
			effect := runtimeEffectIntent{Repository: "o/r", Issue: 353, Attempt: 1, IssueGeneration: 1, AttemptGeneration: 2, IntentRevision: 1, IntentEpoch: 1, Action: string(agentruntime.EffectStop), State: "pending"}
			if kind == "stop" || kind == "completed superseded binding" {
				delete(state.Tombstones, key)
				effect.SupersededReviewerID = strings.Repeat("c", 32)
				effect.SupersededReviewerMode = agentruntime.ReviewModeImplementation
				effect.SupersededReviewerTarget = strings.Repeat("a", 40) + ".." + strings.Repeat("b", 40)
				effect.SupersededReviewerIssueGeneration, effect.SupersededReviewerAttemptGeneration = 1, 1
				effect.SupersededReviewerGateProtocol = true
				effect.SupersededReviewerRequestDigest = strings.Repeat("d", 64)
				if kind == "completed superseded binding" {
					effect.State = "completed"
				}
				effect.ID = runtimeEffectID(effect)
				record := runtimeAttemptRecord{Generation: 2, Manifest: manifest}
				if effect.State == "pending" {
					record.StopEffectID = effect.ID
				}
				state.Attempts[key] = record
				state.Effects[effect.ID] = effect
			} else if kind == "reviewer proof" {
				proof := reviewerProcessProof{Repository: "o/r", Issue: 353, Attempt: 1, IssueGeneration: 1, AttemptGeneration: 1, Mode: agentruntime.ReviewModeImplementation, Target: strings.Repeat("a", 40) + ".." + strings.Repeat("b", 40), EffectID: strings.Repeat("c", 32), GroupPID: 12345, LegacyUnverified: true}
				state.ReviewerProofs[reviewerProofKey(proof.Repository, proof.Issue, proof.Attempt, proof.Mode, proof.Target)] = proof
			} else {
				effect.Action, effect.State = string(agentruntime.EffectCleanup), "completed"
				if kind == "cleanup" {
					effect.State = "pending"
				}
				effect.ID = runtimeEffectID(effect)
				state.Effects[effect.ID] = effect
				tombstone := state.Tombstones[key]
				tombstone.Action, tombstone.Manifest, tombstone.EffectID = "archived", &manifest, effect.ID
				tombstone.CleanupPolicy = &agentruntime.EffectCleanupPolicy{Action: "archive"}
				if kind == "cleanup" {
					tombstone.CleanupPhase = "pending"
				}
				if kind == "start candidate" {
					tombstone.InvalidatedStart = &startCandidateInvalidation{EffectID: strings.Repeat("d", 32), Manifest: manifest, Candidates: []startGateCandidate{{Nonce: strings.Repeat("e", 32), MayRun: true}}}
				}
				if kind == "handoff candidate" {
					tombstone.InvalidatedHandoff = &handoffCandidateInvalidation{EffectID: strings.Repeat("d", 32), Token: strings.Repeat("e", 32), Key: "old-handoff", Manifest: manifest}
				}
				state.Tombstones[key] = tombstone
			}
			before := cloneRuntimeOwnerState(state)
			state = restartLegacyBootOwner(t, root, state, testBootA)
			state = restartLegacyBootOwner(t, root, state, testBootB)
			state = restartLegacyBootOwner(t, root, state, testBootB)
			if len(state.LegacyReviewerQuarantines) != 0 {
				t.Fatal("legacy restriction was not released")
			}
			if !reflect.DeepEqual(state.Effects, before.Effects) || !reflect.DeepEqual(state.ReviewerProofs, before.ReviewerProofs) || !reflect.DeepEqual(state.Tombstones, before.Tombstones) || !reflect.DeepEqual(state.Attempts, before.Attempts) {
				t.Fatal("boot release changed an independent safety obligation")
			}
			status, err := projectOwnerStatus(stateOwnerSnapshot{State: state}, 1, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			if kind == "stop" && (!effectReviewerCleanupBlocked(state, state.Effects[effect.ID]) || len(status.Statuses) != 1 || status.Statuses[0].CurrentPhase != "stop-pending") {
				t.Fatal("Stop lost its safety restriction")
			}
			if kind == "reviewer proof" && !issueHasUnprovedReviewer(state, "o/r", 353) {
				t.Fatal("retained proof no longer blocks dispatch")
			}
			if kind == "start candidate" || kind == "handoff candidate" || kind == "reviewer proof" {
				if len(status.Statuses) != 1 || !status.Statuses[0].OperatorBlocked || status.Statuses[0].CurrentPhase != "physical-unverified" {
					t.Fatalf("independent physical warning disappeared: %#v", status.Statuses)
				}
			}
			if kind == "completed superseded binding" {
				changed := state.Effects[effect.ID]
				delete(state.Effects, effect.ID)
				changed.IntentRevision++
				changed.ID = runtimeEffectID(changed)
				state.Effects[changed.ID] = changed
				if err := validateRuntimeOwnerState(state, runtimeOwnerAttemptRoot(root), root, true); err == nil {
					t.Fatal("release authorized a different effect identity")
				}
			}
		})
	}
}
