package runtime

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestEffectExecutorPrepareAndStartNeverWritesManifest(t *testing.T) {
	r, fake, attempt, _ := testRuntime(t)
	manifest, err := PreparingManifest(r.Root, r.StateRoot, attempt, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	executor := EffectExecutor{Runtime: r}
	prepare := effectTestRequest(t, executor, EffectRequest{Action: EffectPrepare, Attempt: attempt, Manifest: manifest, Eligible: true}, "a")
	prepared, err := executor.Execute(t.Context(), prepare)
	if err != nil || prepared.Manifest.State != "preparing" {
		t.Fatalf("prepared=%#v err=%v", prepared, err)
	}
	manifestPath := filepath.Join(filepath.Dir(manifest.LogPath), "manifest.json")
	if info, err := os.Stat(filepath.Dir(manifestPath)); err != nil || !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
		t.Fatalf("private attempt state directory info=%v err=%v", info, err)
	}
	if _, err := os.Lstat(manifestPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("effect wrote authoritative manifest: %v", err)
	}
	if verification, err := executor.VerifyPending(t.Context(), prepare); err != nil || verification.Disposition != EffectVerified || verification.Result == nil || verification.Result.Manifest.State != "preparing" {
		t.Fatalf("prepare verification=%#v err=%v", verification, err)
	}
	prepareMarker := filepath.Join(r.StateRoot, "runtime-effects", prepare.Identity.EffectID+".done")
	if err := os.Remove(prepareMarker); err != nil {
		t.Fatal(err)
	}
	if verification, err := executor.VerifyPending(t.Context(), prepare); err != nil || verification.Disposition != EffectVerified || verification.Result == nil || verification.Result.Manifest.State != "preparing" {
		t.Fatalf("postcondition-only prepare verification=%#v err=%v", verification, err)
	}

	start := effectTestRequest(t, executor, EffectRequest{Action: EffectStart, Attempt: attempt, Manifest: prepared.Manifest, Eligible: true}, "b")
	started, err := executor.Execute(t.Context(), start)
	if err != nil || started.Manifest.State != "running" || fake.sessions[manifest.Session] == nil {
		t.Fatalf("started=%#v err=%v sessions=%#v", started, err, fake.sessions)
	}
	if verification, err := executor.VerifyPending(t.Context(), start); err != nil || verification.Disposition != EffectVerified || verification.Result == nil || verification.Result.Manifest.State != "running" {
		t.Fatalf("start verification=%#v err=%v", verification, err)
	}
	changed := start
	changed.Attempt.Context = "new current context"
	r.Tmux = "changed-current-tmux"
	if verification, err := executor.VerifyPending(t.Context(), changed); err != nil || verification.Disposition != EffectVerified || verification.Result == nil || verification.Result.Manifest.State != "running" {
		t.Fatalf("marker recovery depended on changed input: %#v err=%v", verification, err)
	}
	if _, err := os.Lstat(manifestPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("start effect wrote authoritative manifest: %v", err)
	}
}

func TestEffectVerificationRejectsChangedRuntimeAndAllowedEnvironment(t *testing.T) {
	t.Run("source", func(t *testing.T) {
		r, _, attempt, _ := testRuntime(t)
		manifest, err := PreparingManifest(r.Root, r.StateRoot, attempt, time.Unix(5, 0))
		if err != nil {
			t.Fatal(err)
		}
		executor := EffectExecutor{Runtime: r}
		request := effectTestRequest(t, executor, EffectRequest{Action: EffectPrepare, Attempt: attempt, Manifest: manifest, Eligible: true}, "1")
		r.Source = filepath.Join(t.TempDir(), "changed-source")
		if _, err := executor.VerifyPending(t.Context(), request); err == nil || !strings.Contains(err.Error(), "digest") {
			t.Fatalf("changed source err=%v", err)
		}
	})
	t.Run("allowed environment", func(t *testing.T) {
		t.Setenv("TEST_EFFECT_ALLOWED", "first")
		r, _, attempt, _ := testRuntime(t)
		r.AllowEnv = append(r.AllowEnv, "TEST_EFFECT_ALLOWED")
		manifest, err := PreparingManifest(r.Root, r.StateRoot, attempt, time.Unix(6, 0))
		if err != nil {
			t.Fatal(err)
		}
		manifest.State = "running"
		executor := EffectExecutor{Runtime: r}
		request := effectTestRequest(t, executor, EffectRequest{Action: EffectMonitor, Attempt: attempt, Manifest: manifest, Eligible: true}, "2")
		t.Setenv("TEST_EFFECT_ALLOWED", "second")
		if _, err := executor.VerifyPending(t.Context(), request); err == nil || !strings.Contains(err.Error(), "digest") {
			t.Fatalf("changed allowed environment err=%v", err)
		}
	})
}

func TestEffectExecutorPersistsDefinitiveStartFailure(t *testing.T) {
	r, fake, attempt, _ := testRuntime(t)
	manifest, err := PreparingManifest(r.Root, r.StateRoot, attempt, time.Unix(7, 0))
	if err != nil {
		t.Fatal(err)
	}
	executor := EffectExecutor{Runtime: r}
	prepare := effectTestRequest(t, executor, EffectRequest{Action: EffectPrepare, Attempt: attempt, Manifest: manifest, Eligible: true}, "3")
	prepared, err := executor.Execute(t.Context(), prepare)
	if err != nil {
		t.Fatal(err)
	}
	fake.fail = "respawn-pane"
	start := effectTestRequest(t, executor, EffectRequest{Action: EffectStart, Attempt: attempt, Manifest: prepared.Manifest, Eligible: true}, "4")
	failed, err := executor.Execute(t.Context(), start)
	if err == nil || failed.Disposition != EffectResultReady || failed.Manifest.State != "failed" || failed.Manifest.Diagnostic == "" {
		t.Fatalf("failed=%#v err=%v", failed, err)
	}
	verification, verifyErr := executor.VerifyPending(t.Context(), start)
	if verifyErr != nil || verification.Disposition != EffectVerified || verification.Result == nil || verification.Result.Manifest.State != "failed" {
		t.Fatalf("verification=%#v err=%v", verification, verifyErr)
	}
}

func TestEffectExecuteUsesBoundRuntimeSnapshot(t *testing.T) {
	t.Setenv("TEST_EFFECT_BOUND", "first")
	r, _, attempt, _ := testRuntime(t)
	r.AllowEnv = append(r.AllowEnv, "TEST_EFFECT_BOUND")
	manifest, err := PreparingManifest(r.Root, r.StateRoot, attempt, time.Unix(8, 0))
	if err != nil {
		t.Fatal(err)
	}
	executor := EffectExecutor{Runtime: r}
	request := effectTestRequest(t, executor, EffectRequest{Action: EffectPrepare, Attempt: attempt, Manifest: manifest, Eligible: true}, "5")
	r.Source = filepath.Join(t.TempDir(), "changed-source")
	t.Setenv("TEST_EFFECT_BOUND", "second")
	result, err := executor.Execute(t.Context(), request)
	if err != nil || result.Manifest.State != "preparing" || result.Disposition != EffectResultReady {
		t.Fatalf("result=%#v err=%v", result, err)
	}
	if !slices.Contains(request.Runtime.Environment, "TEST_EFFECT_BOUND=first") || slices.Contains(request.Runtime.Environment, "TEST_EFFECT_BOUND=second") {
		t.Fatalf("captured environment=%q", request.Runtime.Environment)
	}
}

func TestEffectResultMarkerIsBoundedValidatedAndImmutable(t *testing.T) {
	r, _, attempt, _ := testRuntime(t)
	manifest, err := PreparingManifest(r.Root, r.StateRoot, attempt, time.Unix(4, 0))
	if err != nil {
		t.Fatal(err)
	}
	executor := EffectExecutor{Runtime: r}
	request := effectTestRequest(t, executor, EffectRequest{Action: EffectReview, Attempt: attempt, Manifest: manifest, Eligible: true, Review: ReviewTransition{State: "clean"}}, "f")
	result, err := executor.Execute(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	changed := result
	changed.Manifest.Diagnostic = "different"
	if err := r.writeEffectMarker(changed); err == nil || !strings.Contains(err.Error(), "immutable") {
		t.Fatalf("second marker write err=%v", err)
	}
	mismatched := request
	mismatched.Identity.Epoch++
	if _, err := executor.VerifyPending(t.Context(), mismatched); err == nil || !strings.Contains(err.Error(), "conflicts") {
		t.Fatalf("mismatched marker err=%v", err)
	}
	path := filepath.Join(r.StateRoot, "runtime-effects", request.Identity.EffectID+".done")
	if err := os.WriteFile(path, []byte(`{"version":1,"result":{"Action":"review"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := (EffectExecutor{Runtime: r}).VerifyPending(t.Context(), request); err == nil || !strings.Contains(err.Error(), "marker") {
		t.Fatalf("tampered marker err=%v", err)
	}
}

func TestEffectVerificationRejectsChangedInputAndDoesNotInferFromLiveness(t *testing.T) {
	r, fake, attempt, _ := testRuntime(t)
	manifest, err := PreparingManifest(r.Root, r.StateRoot, attempt, time.Unix(2, 0))
	if err != nil {
		t.Fatal(err)
	}
	executor := EffectExecutor{Runtime: r}
	start := effectTestRequest(t, executor, EffectRequest{Action: EffectStart, Attempt: attempt, Manifest: manifest, Eligible: true}, "c")
	fake.sessions[manifest.Session] = &fakeSession{}
	verification, err := executor.VerifyPending(t.Context(), start)
	if err != nil || verification.Disposition != EffectPending || verification.Result != nil {
		t.Fatalf("live session was treated as completion: %#v err=%v", verification, err)
	}
	handoff := start
	handoff.Action, handoff.Manifest.State = EffectHandoff, "running"
	handoff = effectTestRequest(t, executor, handoff, "7")
	if verification, err := executor.VerifyPending(t.Context(), handoff); err != nil || verification.Disposition != EffectPending || verification.Result != nil {
		t.Fatalf("live handoff session was treated as completion: %#v err=%v", verification, err)
	}
	monitor := start
	monitor.Action = EffectMonitor
	monitor.Manifest.State = "running"
	monitor = effectTestRequest(t, executor, monitor, "d")
	if verification, err = executor.VerifyPending(t.Context(), monitor); err != nil || verification.Disposition != EffectRetry || verification.Result != nil {
		t.Fatalf("monitor verification=%#v err=%v", verification, err)
	}
	changed := start
	changed.Attempt.Context = "changed context"
	if _, err := executor.VerifyPending(t.Context(), changed); err == nil || !strings.Contains(err.Error(), "digest") {
		t.Fatalf("changed request err=%v", err)
	}
	review := effectTestRequest(t, executor, EffectRequest{Action: EffectReview, Attempt: attempt, Manifest: manifest, Eligible: true, Review: ReviewTransition{State: "clean"}}, "6")
	changed = review
	changed.Review.State = "running"
	if _, err := executor.VerifyPending(t.Context(), changed); err == nil || !strings.Contains(err.Error(), "digest") {
		t.Fatalf("changed review request err=%v", err)
	}
}

func TestEffectVerificationReconstructsStopFromDurableReason(t *testing.T) {
	r, _, attempt, _ := testRuntime(t)
	manifest, err := PreparingManifest(r.Root, r.StateRoot, attempt, time.Unix(3, 0))
	if err != nil {
		t.Fatal(err)
	}
	manifest.State = "running"
	executor := EffectExecutor{Runtime: r}
	request := effectTestRequest(t, executor, EffectRequest{Action: EffectStop, Attempt: attempt, Manifest: manifest, Reason: "issue closed"}, "e")
	verification, err := executor.VerifyPending(t.Context(), request)
	if err != nil || verification.Disposition != EffectVerified || verification.Result == nil || verification.Result.Manifest.State != "cancelled" || verification.Result.Manifest.Diagnostic != "issue closed" {
		t.Fatalf("verification=%#v err=%v", verification, err)
	}
}

func effectTestRequest(t *testing.T, executor EffectExecutor, request EffectRequest, id string) EffectRequest {
	t.Helper()
	var err error
	request, err = executor.BindRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	request.Identity = EffectIdentity{Repository: request.Manifest.Repository, Issue: request.Manifest.Issue, Attempt: request.Manifest.Attempt, Epoch: 1, SourceRevision: 2, IssueGeneration: 1, AttemptGeneration: 1, EffectID: strings.Repeat(id, 32)}
	digest, err := EffectRequestDigest(request)
	if err != nil {
		t.Fatal(err)
	}
	request.Identity.RequestDigest = digest
	return request
}
