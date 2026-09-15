package runtime

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

type cancelAfterNewSessionRunner struct {
	*fakeRunner
	cancel context.CancelFunc
}

type replaceBeforeStartBufferRunner struct {
	*fakeRunner
	session string
	foreign *fakeSession
	swapped bool
}

func (runner *replaceBeforeStartBufferRunner) Run(ctx context.Context, command Command) (Result, error) {
	if len(command.Args) > 5 && command.Args[0] == "if-shell" && strings.Contains(command.Args[5], "load-buffer") {
		runner.fakeRunner.sessions[runner.session] = runner.foreign
		runner.swapped = true
		return Result{Output: ImplementationGuardMismatch}, nil
	}
	return runner.fakeRunner.Run(ctx, command)
}

func (runner cancelAfterNewSessionRunner) Run(ctx context.Context, command Command) (Result, error) {
	result, err := runner.fakeRunner.Run(ctx, command)
	if err == nil && slices.Contains(command.Args, "new-session") {
		runner.cancel()
	}
	return result, err
}

func TestHandoffEffectReauthorizesExactParkedCandidateOnReplay(t *testing.T) {
	r, fake, attempt, _ := testRuntime(t)
	manifest, err := prepareAndStartFixture(t, r, t.Context(), attempt)
	if err != nil {
		t.Fatal(err)
	}
	delete(fake.sessions, manifest.Session)
	candidate := manifest
	candidate.LaunchToken = strings.Repeat("c", 32)
	candidate.LaunchID = strings.Repeat("d", 32)
	command := BoundPaneExitStatusCommand(r.Helper, r.tmux(), candidate, []string{"/bin/sh"})
	if err := r.startSession(t.Context(), candidate, nil, candidate.LaunchID, command); err != nil {
		t.Fatal(err)
	}
	request := EffectRequest{Manifest: manifest, Attempt: attempt, Eligible: true, CandidateLaunchToken: candidate.LaunchToken, Identity: EffectIdentity{EffectID: candidate.LaunchID}}
	if !slices.Contains(fake.sessions[manifest.Session].agent, "implementation-gate") {
		t.Fatal("fixture did not park the handoff candidate")
	}
	if _, err := r.handoffEffect(t.Context(), request, func(context.Context, EffectRequest) error {
		return errors.New("stale owner generation")
	}); err == nil {
		t.Fatal("stale owner intent released the candidate")
	}
	if !slices.Contains(fake.sessions[manifest.Session].agent, "implementation-gate") {
		t.Fatal("authorization failure released the parked candidate")
	}
	resumed, err := r.handoffEffect(t.Context(), request, func(context.Context, EffectRequest) error { return nil })
	if err != nil || resumed.LaunchID != candidate.LaunchID || resumed.LaunchToken != candidate.LaunchToken || resumed.State != "running" {
		t.Fatalf("handoff replay result=%#v err=%v", resumed, err)
	}
	if slices.Contains(fake.sessions[manifest.Session].agent, "implementation-gate") {
		t.Fatal("authorized replay did not release the exact candidate")
	}
}

func TestPendingStartReplaysOnlyExactParkedPane(t *testing.T) {
	r, fake, attempt, _ := testRuntime(t)
	manifest, err := PreparingManifest(r.Root, r.StateRoot, attempt, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	executor := EffectExecutor{Runtime: r, AuthorizeLaunch: func(context.Context, EffectRequest) error { return nil }}
	prepare := effectTestRequest(t, executor, EffectRequest{Action: EffectPrepare, Attempt: attempt, Manifest: manifest, Eligible: true}, "a")
	prepared, err := executor.Execute(t.Context(), prepare)
	if err != nil {
		t.Fatal(err)
	}
	start := effectTestRequest(t, executor, EffectRequest{Action: EffectStart, Attempt: attempt, Manifest: prepared.Manifest, Eligible: true}, "b")
	candidate := start.Manifest
	candidate.LaunchID = start.Identity.EffectID
	if err := r.startSession(t.Context(), candidate, nil, candidate.LaunchID, []string{"/bin/sh"}); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(fake.sessions[manifest.Session].agent, "implementation-gate") {
		t.Fatal("fixture did not park the Start pane")
	}
	if verification, err := executor.VerifyPending(t.Context(), start); err != nil || verification.Disposition != EffectRetry {
		t.Fatalf("exact parked Start was not replayable: %#v, %v", verification, err)
	}
	before := len(fake.seen)
	result, err := executor.Execute(t.Context(), start)
	if err != nil || result.Disposition != EffectResultReady || result.Manifest.State != "running" {
		t.Fatalf("parked Start replay result=%#v err=%v", result, err)
	}
	for _, command := range fake.seen[before:] {
		if slices.Contains(command.Args, "new-session") {
			t.Fatalf("parked Start replay created a second pane: %#v", command)
		}
	}
	if slices.Contains(fake.sessions[manifest.Session].agent, "implementation-gate") {
		t.Fatal("owner-authorized Start did not release the exact parked pane")
	}
}

func TestStartLoadsContextOnlyThroughBoundEffectBuffer(t *testing.T) {
	r, fake, attempt, _ := testRuntime(t)
	manifest, err := PreparingManifest(r.Root, r.StateRoot, attempt, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	executor := EffectExecutor{Runtime: r, AuthorizeLaunch: func(context.Context, EffectRequest) error { return nil }}
	prepare := effectTestRequest(t, executor, EffectRequest{Action: EffectPrepare, Attempt: attempt, Manifest: manifest, Eligible: true}, "6")
	prepared, err := executor.Execute(t.Context(), prepare)
	if err != nil {
		t.Fatal(err)
	}
	start := effectTestRequest(t, executor, EffectRequest{Action: EffectStart, Attempt: attempt, Manifest: prepared.Manifest, Eligible: true}, "7")
	result, err := executor.Execute(t.Context(), start)
	if err != nil || result.Manifest.State != "running" {
		t.Fatalf("bound Start result=%#v err=%v", result, err)
	}
	if fake.buffers["as-start-"+start.Identity.EffectID] != attempt.Context || fake.buffers[manifest.Session] != "" {
		t.Fatalf("Start context used a shared buffer: %#v", fake.buffers)
	}
	guardedLoad := false
	for index, command := range fake.seen {
		if len(command.Args) == 0 || command.Args[0] != "load-buffer" {
			continue
		}
		if index == 0 || len(fake.seen[index-1].Args) == 0 || fake.seen[index-1].Args[0] != "if-shell" {
			t.Fatalf("Start context loaded without a pane guard: %#v", command)
		}
		guardedLoad = true
	}
	if !guardedLoad {
		t.Fatal("Start did not load its context")
	}
}

func TestStartDoesNotLoadContextAfterPaneReplacement(t *testing.T) {
	r, fake, attempt, _ := testRuntime(t)
	manifest, err := PreparingManifest(r.Root, r.StateRoot, attempt, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	executor := EffectExecutor{Runtime: r, AuthorizeLaunch: func(context.Context, EffectRequest) error { return nil }}
	prepare := effectTestRequest(t, executor, EffectRequest{Action: EffectPrepare, Attempt: attempt, Manifest: manifest, Eligible: true}, "8")
	prepared, err := executor.Execute(t.Context(), prepare)
	if err != nil {
		t.Fatal(err)
	}
	foreign := &fakeSession{paneID: "%9", worktree: manifest.Worktree}
	runner := &replaceBeforeStartBufferRunner{fakeRunner: fake, session: manifest.Session, foreign: foreign}
	r.Runner = runner
	start := effectTestRequest(t, executor, EffectRequest{Action: EffectStart, Attempt: attempt, Manifest: prepared.Manifest, Eligible: true}, "a")
	result, err := executor.Execute(t.Context(), start)
	if !runner.swapped || err == nil || result.Disposition != EffectResultAmbiguous {
		t.Fatalf("replacement was not rejected before context load: swapped=%t result=%#v err=%v", runner.swapped, result, err)
	}
	if fake.sessions[manifest.Session] != foreign || len(fake.buffers) != 0 {
		t.Fatalf("replacement received context or was mutated: session=%#v buffers=%#v", fake.sessions[manifest.Session], fake.buffers)
	}
}

func TestPendingStartDoesNotDuplicateRenamedLivePane(t *testing.T) {
	r, fake, attempt, _ := testRuntime(t)
	manifest, err := PreparingManifest(r.Root, r.StateRoot, attempt, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	executor := EffectExecutor{Runtime: r, AuthorizeLaunch: func(context.Context, EffectRequest) error { return nil }}
	prepare := effectTestRequest(t, executor, EffectRequest{Action: EffectPrepare, Attempt: attempt, Manifest: manifest, Eligible: true}, "c")
	prepared, err := executor.Execute(t.Context(), prepare)
	if err != nil {
		t.Fatal(err)
	}
	start := effectTestRequest(t, executor, EffectRequest{Action: EffectStart, Attempt: attempt, Manifest: prepared.Manifest, Eligible: true}, "d")
	candidate := start.Manifest
	candidate.LaunchID = start.Identity.EffectID
	if err := r.startSession(t.Context(), candidate, nil, candidate.LaunchID, []string{"/bin/sh"}); err != nil {
		t.Fatal(err)
	}
	bound := fake.sessions[manifest.Session]
	delete(fake.sessions, manifest.Session)
	fake.sessions["renamed-live-pane"] = bound // The original pane survives; only its session name changes.
	if verification, err := executor.VerifyPending(t.Context(), start); err != nil || verification.Disposition != EffectRotate {
		t.Errorf("renamed live pane incorrectly authorized same-candidate retry: %#v, %v", verification, err)
	}
	before := len(fake.seen)
	_, _ = executor.Execute(t.Context(), start)
	for _, command := range fake.seen[before:] {
		if slices.Contains(command.Args, "new-session") {
			t.Fatalf("Start created a second pane while its original bound pane lives: %#v", command)
		}
	}
	if fake.sessions["renamed-live-pane"] != bound {
		t.Fatal("recovery changed the original live pane")
	}
}

func TestPendingStartBeforeSessionCreationStaysUnproved(t *testing.T) {
	r, fake, attempt, _ := testRuntime(t)
	manifest, err := PreparingManifest(r.Root, r.StateRoot, attempt, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	executor := EffectExecutor{Runtime: r, AuthorizeLaunch: func(context.Context, EffectRequest) error { return nil }}
	start := effectTestRequest(t, executor, EffectRequest{Action: EffectStart, Attempt: attempt, Manifest: manifest, Eligible: true}, "9")
	verification, err := executor.VerifyPending(t.Context(), start)
	if err != nil || verification.Disposition != EffectRotate {
		t.Fatalf("pre-session Start did not require owner candidate rotation: %#v, %v", verification, err)
	}
	if len(fake.sessions) != 0 {
		t.Fatalf("verification created a session: %#v", fake.sessions)
	}
}

func TestPendingStartDoesNotAdoptUnboundSameNamePane(t *testing.T) {
	r, fake, attempt, _ := testRuntime(t)
	manifest, err := PreparingManifest(r.Root, r.StateRoot, attempt, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	executor := EffectExecutor{Runtime: r, AuthorizeLaunch: func(context.Context, EffectRequest) error { return nil }}
	start := effectTestRequest(t, executor, EffectRequest{Action: EffectStart, Attempt: attempt, Manifest: manifest, Eligible: true}, "e")
	foreign := &fakeSession{paneID: "%8", worktree: manifest.Worktree}
	fake.sessions[manifest.Session] = foreign // Crash before the immutable binding was fsynced, or a replacement pane.
	if verification, err := executor.VerifyPending(t.Context(), start); err != nil || verification.Disposition != EffectPending {
		t.Fatalf("unbound same-name pane was not quarantined: %#v, %v", verification, err)
	}
	before := len(fake.seen)
	if _, err := executor.Execute(t.Context(), start); err == nil {
		t.Fatal("unbound same-name pane was adopted as an owner-bound Start")
	}
	for _, command := range fake.seen[before:] {
		if slices.Contains(command.Args, "new-session") || slices.Contains(command.Args, "kill-pane") {
			t.Fatalf("recovery mutated an unbound pane: %#v", command)
		}
	}
	if fake.sessions[manifest.Session] != foreign {
		t.Fatal("recovery replaced the unbound pane")
	}
}

func TestCanceledStartAfterNewSessionDoesNotReleaseWorker(t *testing.T) {
	r, fake, attempt, _ := testRuntime(t)
	fake.sessions["keeper"] = &fakeSession{paneID: "%999"} // Keep the original server available for exact pane-absence proof.
	manifest, err := PreparingManifest(r.Root, r.StateRoot, attempt, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	executor := EffectExecutor{Runtime: r, AuthorizeLaunch: func(context.Context, EffectRequest) error { return nil }}
	prepare := effectTestRequest(t, executor, EffectRequest{Action: EffectPrepare, Attempt: attempt, Manifest: manifest, Eligible: true}, "f")
	prepared, err := executor.Execute(t.Context(), prepare)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	r.Runner = cancelAfterNewSessionRunner{fakeRunner: fake, cancel: cancel}
	start := effectTestRequest(t, executor, EffectRequest{Action: EffectStart, Attempt: attempt, Manifest: prepared.Manifest, Eligible: true}, "1")
	result, err := executor.Execute(ctx, start)
	if !errors.Is(err, context.Canceled) || result.Disposition != EffectResultAmbiguous || result.Manifest.State != "preparing" {
		t.Fatalf("canceled Start result=%#v err=%v", result, err)
	}
	if fake.sessions[manifest.Session] == nil || !slices.Contains(fake.sessions[manifest.Session].agent, "implementation-gate") {
		t.Fatal("canceled Start did not preserve its parked candidate for owner recovery")
	}
	for _, command := range fake.seen {
		if slices.Contains(command.Args, "wait-for") && slices.Contains(command.Args, "-U") {
			t.Fatalf("canceled Start released its worker: %#v", command)
		}
	}
}

func TestEffectPrepareRejectsMissingBoundWorkerHelperBeforeExternalWork(t *testing.T) {
	r, fake, attempt, _ := testRuntime(t)
	r.Helper = ""
	attempt.Context = "" // No prompt path may fall back to an unbound raw worker.
	manifest, err := PreparingManifest(r.Root, r.StateRoot, attempt, time.Unix(8, 0))
	if err != nil {
		t.Fatal(err)
	}
	executor := EffectExecutor{Runtime: r, AuthorizeLaunch: func(context.Context, EffectRequest) error { return nil }}
	request := effectTestRequest(t, executor, EffectRequest{Action: EffectPrepare, Attempt: attempt, Manifest: manifest, Eligible: true}, "e")
	if _, err := executor.Execute(t.Context(), request); err == nil || !strings.Contains(err.Error(), "helper") {
		t.Fatalf("unbound worker helper was accepted: %v", err)
	}
	if len(fake.seen) != 0 {
		t.Fatalf("missing helper was rejected after external work: %#v", fake.seen)
	}
	if _, err := os.Lstat(manifest.Worktree); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing helper created worktree: %v", err)
	}
}

func TestEffectExecutorHandoffRecreatesMissingSessionWithBoundHelper(t *testing.T) {
	r, fake, attempt, _ := testRuntime(t)
	manifest, err := prepareAndStartFixture(t, r, t.Context(), attempt)
	if err != nil {
		t.Fatal(err)
	}
	r.Root, err = filepath.EvalSymlinks(r.Root)
	if err != nil {
		t.Fatal(err)
	}
	delete(fake.sessions, manifest.Session)
	fake.sessions["keeper"] = &fakeSession{paneID: "%999"}
	executor := EffectExecutor{Runtime: r, AuthorizeLaunch: func(context.Context, EffectRequest) error { return nil }}
	request := effectTestRequest(t, executor, EffectRequest{Action: EffectHandoff, Attempt: attempt, Manifest: manifest, Eligible: true, CandidateLaunchToken: strings.Repeat("c", 32)}, "d")
	result, err := executor.Execute(t.Context(), request)
	if err != nil || result.Disposition != EffectResultReady || result.Manifest.State != "running" || result.Manifest.LaunchID != request.Identity.EffectID || result.Manifest.LaunchToken != request.CandidateLaunchToken {
		t.Fatalf("bound handoff launch=%#v err=%v", result, err)
	}
	if binding, err := ReadImplementationBinding(result.Manifest); err != nil || binding.Role != "interactive" || binding.Token != request.CandidateLaunchToken {
		t.Fatalf("bound handoff helper identity=%#v err=%v", binding, err)
	}
}

func TestPersistedEmptyHelperHandoffDoesNotChangeDigestOrLaunchRawWorker(t *testing.T) {
	r, fake, attempt, _ := testRuntime(t)
	manifest, err := prepareAndStartFixture(t, r, t.Context(), attempt)
	if err != nil {
		t.Fatal(err)
	}
	r.Root, err = filepath.EvalSymlinks(r.Root)
	if err != nil {
		t.Fatal(err)
	}
	delete(fake.sessions, manifest.Session)
	executor := EffectExecutor{Runtime: r, AuthorizeLaunch: func(context.Context, EffectRequest) error { return nil }}
	request := effectTestRequest(t, executor, EffectRequest{Action: EffectHandoff, Attempt: attempt, Manifest: manifest, Eligible: true, CandidateLaunchToken: strings.Repeat("c", 32)}, "d")
	request.Runtime.Helper = "" // Persisted by the predecessor binary.
	request.Identity.RequestDigest, err = EffectRequestDigest(request)
	if err != nil {
		t.Fatal(err)
	}
	if verification, err := executor.VerifyPending(t.Context(), request); err != nil || verification.Disposition != EffectRetry {
		t.Fatalf("legacy handoff digest changed on recovery: %#v, %v", verification, err)
	}
	before := len(fake.seen)
	if _, err := executor.Execute(t.Context(), request); err == nil || !strings.Contains(err.Error(), "helper") {
		t.Fatalf("legacy handoff recreated unbound worker: %v", err)
	}
	for _, command := range fake.seen[before:] {
		if slices.Contains(command.Args, "new-session") {
			t.Fatalf("legacy handoff launched a raw worker: %#v", command)
		}
	}
}

func TestEffectExecutorMonitorNoopAndTerminalResult(t *testing.T) {
	r, fake, attempt, _ := testRuntime(t)
	manifest, err := PreparingManifest(r.Root, r.StateRoot, attempt, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	executor := EffectExecutor{Runtime: r, AuthorizeLaunch: func(context.Context, EffectRequest) error { return nil }}
	prepare := effectTestRequest(t, executor, EffectRequest{Action: EffectPrepare, Attempt: attempt, Manifest: manifest, Eligible: true}, "a")
	prepared, err := executor.Execute(t.Context(), prepare)
	if err != nil {
		t.Fatal(err)
	}
	start := effectTestRequest(t, executor, EffectRequest{Action: EffectStart, Attempt: attempt, Manifest: prepared.Manifest, Eligible: true}, "b")
	started, err := executor.Execute(t.Context(), start)
	if err != nil {
		t.Fatal(err)
	}
	manifest = started.Manifest
	if binding, err := ReadImplementationBinding(manifest); err != nil || binding.Role != "capture" {
		t.Fatalf("successful launch lacks capture role: %#v, %v", binding, err)
	}
	request := effectTestRequest(t, executor, EffectRequest{Action: EffectMonitor, Attempt: attempt, Manifest: manifest, Eligible: true}, "c")
	live, err := executor.Execute(t.Context(), request)
	if err != nil || live.Disposition != EffectResultReady || !reflect.DeepEqual(live.Manifest, manifest) {
		t.Fatalf("alive Monitor changed the manifest: result=%#v err=%v", live, err)
	}
	fake.sessions[manifest.Session].dead = true
	terminal := effectTestRequest(t, executor, EffectRequest{Action: EffectMonitor, Attempt: attempt, Manifest: manifest, Eligible: true}, "d")
	finished, err := executor.Execute(t.Context(), terminal)
	if err != nil || finished.Disposition != EffectResultReady || finished.Manifest.State != "completed" || !finished.Manifest.UpdatedAt.After(manifest.UpdatedAt) {
		t.Fatalf("terminal Monitor did not commit a timestamped outcome: result=%#v err=%v", finished, err)
	}
}

func TestEffectExecutorPrepareAndStartNeverWritesManifest(t *testing.T) {
	r, fake, attempt, _ := testRuntime(t)
	manifest, err := PreparingManifest(r.Root, r.StateRoot, attempt, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	executor := EffectExecutor{Runtime: r, AuthorizeLaunch: func(context.Context, EffectRequest) error { return nil }}
	prepare := effectTestRequest(t, executor, EffectRequest{Action: EffectPrepare, Attempt: attempt, Manifest: manifest, Eligible: true}, "a")
	prepared, err := executor.Execute(t.Context(), prepare)
	if err != nil || prepared.Manifest.State != "preparing" {
		t.Fatalf("prepared=%#v err=%v", prepared, err)
	}
	if temporary, err := filepath.Glob(filepath.Join(r.StateRoot, "runtime-effects", ".effect-*")); err != nil || len(temporary) != 0 {
		t.Fatalf("runtime marker temporaries=%v err=%v", temporary, err)
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

func TestReclaimOrphanEffectMarkersRetainsPendingAndFailsClosed(t *testing.T) {
	r, _, attempt, _ := testRuntime(t)
	manifest, err := PreparingManifest(r.Root, r.StateRoot, attempt, time.Unix(2, 0))
	if err != nil {
		t.Fatal(err)
	}
	executor := EffectExecutor{Runtime: r, AuthorizeLaunch: func(context.Context, EffectRequest) error { return nil }}
	request := effectTestRequest(t, executor, EffectRequest{Action: EffectPrepare, Attempt: attempt, Manifest: manifest, Eligible: true}, "c")
	result, err := executor.Execute(t.Context(), request)
	if err != nil || result.Disposition != EffectResultReady {
		t.Fatalf("result=%#v err=%v", result, err)
	}
	path := filepath.Join(r.StateRoot, "runtime-effects", request.Identity.EffectID+".done")
	temporary := filepath.Join(r.StateRoot, "runtime-effects", ".effect-12345")
	if err := os.WriteFile(temporary, []byte("crash residue"), 0o600); err != nil {
		t.Fatal(err)
	}
	restarted := &Runtime{Root: r.Root, StateRoot: r.StateRoot}
	if err := restarted.ReclaimOrphanEffectMarkers(map[string]bool{request.Identity.EffectID: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(path); err != nil {
		t.Fatalf("pending marker was removed: %v", err)
	}
	if _, err := os.Lstat(temporary); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("validated crash temporary was not removed: %v", err)
	}
	unsafeTemporary := filepath.Join(r.StateRoot, "runtime-effects", ".effect-67890")
	if err := os.WriteFile(unsafeTemporary, []byte("unsafe"), 0o644); err != nil {
		t.Fatal(err)
	}
	unsafe := filepath.Join(r.StateRoot, "runtime-effects", strings.Repeat("d", 32)+".done")
	if err := os.WriteFile(unsafe, []byte("not-json\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := r.ReclaimOrphanEffectMarkers(nil); err == nil {
		t.Fatal("malformed marker directory did not fail closed")
	}
	if _, err := os.Lstat(path); err != nil {
		t.Fatalf("valid orphan was removed before full validation: %v", err)
	}
	if _, err := os.Lstat(unsafeTemporary); err != nil {
		t.Fatalf("unsafe temporary was removed before full validation: %v", err)
	}
	if err := os.Remove(unsafe); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(unsafeTemporary); err != nil {
		t.Fatal(err)
	}
	symlinkTemporary := filepath.Join(r.StateRoot, "runtime-effects", ".effect-13579")
	if err := os.Symlink(path, symlinkTemporary); err != nil {
		t.Fatal(err)
	}
	if err := restarted.ReclaimOrphanEffectMarkers(nil); err == nil {
		t.Fatal("symlink temporary did not fail closed")
	}
	if err := os.Remove(symlinkTemporary); err != nil {
		t.Fatal(err)
	}
	unknownTemporary := filepath.Join(r.StateRoot, "runtime-effects", ".effect-invalid")
	if err := os.WriteFile(unknownTemporary, []byte("unknown"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := restarted.ReclaimOrphanEffectMarkers(nil); err == nil {
		t.Fatal("unknown temporary namespace did not fail closed")
	}
	if err := os.Remove(unknownTemporary); err != nil {
		t.Fatal(err)
	}
	if err := restarted.ReclaimOrphanEffectMarkers(nil); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("orphan marker was not removed: %v", err)
	}
	temporary = filepath.Join(r.StateRoot, "runtime-effects", ".effect-24680")
	if err := os.WriteFile(temporary, []byte("crash residue"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := restarted.ReclaimOrphanEffectMarkers(nil); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(temporary); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temp-only crash residue was not removed: %v", err)
	}
}

func TestEffectVerificationRejectsChangedRuntimeAndAllowedEnvironment(t *testing.T) {
	t.Run("source", func(t *testing.T) {
		r, _, attempt, _ := testRuntime(t)
		manifest, err := PreparingManifest(r.Root, r.StateRoot, attempt, time.Unix(5, 0))
		if err != nil {
			t.Fatal(err)
		}
		executor := EffectExecutor{Runtime: r, AuthorizeLaunch: func(context.Context, EffectRequest) error { return nil }}
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
		executor := EffectExecutor{Runtime: r, AuthorizeLaunch: func(context.Context, EffectRequest) error { return nil }}
		request := effectTestRequest(t, executor, EffectRequest{Action: EffectMonitor, Attempt: attempt, Manifest: manifest, Eligible: true}, "2")
		t.Setenv("TEST_EFFECT_ALLOWED", "second")
		if _, err := executor.VerifyPending(t.Context(), request); err == nil || !strings.Contains(err.Error(), "digest") {
			t.Fatalf("changed allowed environment err=%v", err)
		}
	})
}

func TestEffectExecutorRetainsAmbiguousStartUntilExactGateProof(t *testing.T) {
	r, fake, attempt, _ := testRuntime(t)
	// Keep the original fake tmux server observable after the attempt pane is
	// killed, so cleanup has exact same-server absence proof.
	fake.sessions["keeper"] = &fakeSession{paneID: "%999"}
	manifest, err := PreparingManifest(r.Root, r.StateRoot, attempt, time.Unix(7, 0))
	if err != nil {
		t.Fatal(err)
	}
	executor := EffectExecutor{Runtime: r, AuthorizeLaunch: func(context.Context, EffectRequest) error { return nil }}
	prepare := effectTestRequest(t, executor, EffectRequest{Action: EffectPrepare, Attempt: attempt, Manifest: manifest, Eligible: true}, "3")
	prepared, err := executor.Execute(t.Context(), prepare)
	if err != nil {
		t.Fatal(err)
	}
	fake.fail = "wait-for"
	start := effectTestRequest(t, executor, EffectRequest{Action: EffectStart, Attempt: attempt, Manifest: prepared.Manifest, Eligible: true}, "4")
	partial, err := executor.Execute(t.Context(), start)
	if err == nil || partial.Disposition != EffectResultAmbiguous || partial.Manifest.State != "preparing" {
		t.Fatalf("partial=%#v err=%v", partial, err)
	}
	verification, verifyErr := executor.VerifyPending(t.Context(), start)
	if verifyErr != nil || verification.Disposition != EffectRetry || verification.Result != nil {
		t.Fatalf("verification=%#v err=%v", verification, verifyErr)
	}
}

func TestInteractiveStartReusesSafeReservedResultAfterCandidateRotation(t *testing.T) {
	r, _, attempt, _ := testRuntime(t)
	attempt.Interactive = true
	manifest, err := PreparingManifest(r.Root, r.StateRoot, attempt, time.Unix(7, 0))
	if err != nil {
		t.Fatal(err)
	}
	executor := EffectExecutor{Runtime: r, AuthorizeLaunch: func(context.Context, EffectRequest) error { return nil }}
	prepare := effectTestRequest(t, executor, EffectRequest{Action: EffectPrepare, Attempt: attempt, Manifest: manifest, Eligible: true}, "3")
	prepared, err := executor.Execute(t.Context(), prepare)
	if err != nil {
		t.Fatal(err)
	}
	resultPath := ResultPath(prepared.Manifest.Worktree)
	if err := os.Mkdir(PrivatePath(prepared.Manifest.Worktree), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(resultPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	start := effectTestRequest(t, executor, EffectRequest{Action: EffectStart, Attempt: attempt, Manifest: prepared.Manifest, Eligible: true}, "4")
	started, err := executor.Execute(t.Context(), start)
	if err != nil || started.Manifest.State != "running" {
		t.Fatalf("rotated Start rejected safe reserved result: result=%#v err=%v", started, err)
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
	executor := EffectExecutor{Runtime: r, AuthorizeLaunch: func(context.Context, EffectRequest) error { return nil }}
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
	executor := EffectExecutor{Runtime: r, AuthorizeLaunch: func(context.Context, EffectRequest) error { return nil }}
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
	if err := r.RemoveEffectMarker(request.Identity); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("reclaimed marker remains: %v", err)
	}
}

func TestEffectVerificationRejectsChangedInputAndDoesNotInferFromLiveness(t *testing.T) {
	r, fake, attempt, _ := testRuntime(t)
	manifest, err := PreparingManifest(r.Root, r.StateRoot, attempt, time.Unix(2, 0))
	if err != nil {
		t.Fatal(err)
	}
	executor := EffectExecutor{Runtime: r, AuthorizeLaunch: func(context.Context, EffectRequest) error { return nil }}
	start := effectTestRequest(t, executor, EffectRequest{Action: EffectStart, Attempt: attempt, Manifest: manifest, Eligible: true}, "c")
	fake.sessions[manifest.Session] = &fakeSession{}
	verification, err := executor.VerifyPending(t.Context(), start)
	if err != nil || verification.Disposition != EffectPending || verification.Result != nil {
		t.Fatalf("live session was treated as completion: %#v err=%v", verification, err)
	}
	handoff := start
	handoff.Action, handoff.Manifest.State = EffectHandoff, "running"
	handoff.CandidateLaunchToken = strings.Repeat("e", 32)
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

func TestEffectVerificationDoesNotReconstructLegacyStopFromMissingName(t *testing.T) {
	r, _, attempt, _ := testRuntime(t)
	manifest, err := PreparingManifest(r.Root, r.StateRoot, attempt, time.Unix(3, 0))
	if err != nil {
		t.Fatal(err)
	}
	manifest.State = "running"
	executor := EffectExecutor{Runtime: r, AuthorizeLaunch: func(context.Context, EffectRequest) error { return nil }}
	request := effectTestRequest(t, executor, EffectRequest{Action: EffectStop, Attempt: attempt, Manifest: manifest, Reason: "issue closed"}, "e")
	verification, err := executor.VerifyPending(t.Context(), request)
	if err != nil || verification.Disposition != EffectPending || verification.Result != nil {
		t.Fatalf("verification=%#v err=%v", verification, err)
	}
}

func TestEffectVerificationSettlesGenerationBoundConfinedStopAfterRestart(t *testing.T) {
	r, _, attempt, _ := testRuntime(t)
	profile := strings.Repeat("a", 64)
	r.WorkerProfileDigest = profile
	manifest, err := PreparingManifest(r.Root, r.StateRoot, attempt, time.Unix(3, 0))
	if err != nil {
		t.Fatal(err)
	}
	manifest, err = BindWorkerConfinement(manifest, 7, profile)
	if err != nil {
		t.Fatal(err)
	}
	executor := EffectExecutor{Runtime: r}
	request := effectTestRequest(t, executor, EffectRequest{Action: EffectStop, Attempt: attempt, Manifest: manifest, Reason: "operator cancelled"}, "f")
	request.Identity.AttemptGeneration = 7
	request.Identity.RequestDigest, err = EffectRequestDigest(request)
	if err != nil {
		t.Fatal(err)
	}
	verification, err := executor.VerifyPending(t.Context(), request)
	if err != nil || verification.Disposition != EffectVerified || verification.Result == nil || verification.Result.Manifest.State != "cancelled" {
		t.Fatalf("confined restart verification=%#v err=%v", verification, err)
	}

	wrongProfile := request
	wrongProfile.Runtime.WorkerProfileDigest = strings.Repeat("b", 64)
	wrongProfile.Identity.RequestDigest, err = EffectRequestDigest(wrongProfile)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = executor.VerifyPending(t.Context(), wrongProfile); err == nil {
		t.Fatal("mismatched profile escaped confinement validation")
	}
}

func TestEffectCleanupPolicyIsClosedAndDigestBound(t *testing.T) {
	r, _, attempt, _ := testRuntime(t)
	manifest, err := PreparingManifest(r.Root, r.StateRoot, attempt, time.Unix(4, 0))
	if err != nil {
		t.Fatal(err)
	}
	executor := EffectExecutor{Runtime: r, Cleanup: func(context.Context, EffectRequest) error { return nil }, VerifyCleanup: func(context.Context, EffectRequest) (bool, error) { return true, nil }}
	base, err := executor.BindRequest(EffectRequest{Action: EffectCleanup, Attempt: attempt, Manifest: manifest, Eligible: true, Cleanup: EffectCleanupPolicy{Action: "archive"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := executor.ValidateRequest(base); err != nil {
		t.Fatal(err)
	}
	baseDigest, err := EffectRequestDigest(base)
	if err != nil {
		t.Fatal(err)
	}
	for _, policy := range []EffectCleanupPolicy{{Action: "abandon"}, {Action: "dismiss"}, {Action: "remove", PublishedHead: strings.Repeat("a", 40)}} {
		changed := base
		changed.Cleanup = policy
		if err := executor.ValidateRequest(changed); err != nil {
			t.Fatalf("policy=%#v: %v", policy, err)
		}
		digest, err := EffectRequestDigest(changed)
		if err != nil {
			t.Fatal(err)
		}
		if digest == baseDigest {
			t.Fatalf("cleanup policy was not digest-bound: %#v", policy)
		}
	}
	for _, policy := range []EffectCleanupPolicy{{}, {Action: "dismiss", PublishedHead: "unexpected"}, {Action: "archive", PublishedHead: "unexpected"}, {Action: "remove"}, {Action: "remove", PublishedHead: strings.Repeat("A", 40)}} {
		invalid := base
		invalid.Cleanup = policy
		if err := executor.ValidateRequest(invalid); err == nil {
			t.Fatalf("accepted cleanup policy %#v", policy)
		}
	}
	prepare := EffectRequest{Action: EffectPrepare, Attempt: attempt, Manifest: manifest, Eligible: true, Cleanup: EffectCleanupPolicy{Action: "archive"}}
	prepare, err = executor.BindRequest(prepare)
	if err != nil {
		t.Fatal(err)
	}
	if err := executor.ValidateRequest(prepare); err == nil {
		t.Fatal("accepted cleanup policy on prepare")
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
	if request.Action == EffectStart {
		request.GateNonce = request.Identity.EffectID
	}
	digest, err := EffectRequestDigest(request)
	if err != nil {
		t.Fatal(err)
	}
	request.Identity.RequestDigest = digest
	return request
}
