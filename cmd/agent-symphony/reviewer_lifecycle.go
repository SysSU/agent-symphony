package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"

	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

// The files are durable proof from the exact reviewer wrapper. They are not
// owner state; the owner still validates every completion before committing it.
type reviewerLaunchIdentity struct {
	EffectID          string `json:"effect_id"`
	IssueGeneration   uint64 `json:"issue_generation"`
	AttemptGeneration uint64 `json:"attempt_generation"`
	RequestDigest     string `json:"request_digest"`
	ChildPID          int    `json:"child_pid,omitempty"`
}

type reviewerTerminalRecord struct {
	Identity reviewerLaunchIdentity `json:"identity"`
	ExitCode int                    `json:"exit_code"`
	Signal   int                    `json:"signal,omitempty"`
}

var errReviewerTerminal = errors.New("reviewer terminal failure")

func reviewerIdentity(identity stateResultIdentity) reviewerLaunchIdentity {
	return reviewerLaunchIdentity{EffectID: identity.EffectID, IssueGeneration: identity.IssueGeneration, AttemptGeneration: identity.AttemptGeneration, RequestDigest: identity.RequestDigest}
}

func sameReviewerIdentity(actual, expected reviewerLaunchIdentity) bool {
	actual.ChildPID = 0
	expected.ChildPID = 0
	return reflect.DeepEqual(actual, expected)
}

func reviewerLifecyclePaths(snapshot, target string) (string, string) {
	root := filepath.Dir(reviewResultPath(snapshot, target))
	return filepath.Join(root, "launch.json"), filepath.Join(root, "terminal.json")
}

func reviewerSignal(identity reviewerLaunchIdentity) string { return "review-" + identity.EffectID }
func reviewerStartSignal(identity reviewerLaunchIdentity) string {
	return reviewerSignal(identity) + "-start"
}
func reviewerGoSignal(identity reviewerLaunchIdentity) string {
	return reviewerSignal(identity) + "-go"
}

func reviewerPaneStartMatches(start, launchPath, terminalPath string, identity reviewerLaunchIdentity) bool {
	return strings.Contains(start, " review-pane tmux ") && strings.Contains(start, launchPath) && strings.Contains(start, terminalPath) && strings.Contains(start, reviewerSignal(identity)) && strings.Contains(start, identity.RequestDigest)
}

func reviewerPanePID(output string) (int, error) {
	pid, err := strconv.Atoi(strings.TrimSpace(output))
	if err != nil || pid < 2 {
		return 0, errors.New("exact reviewer pane PID is unavailable")
	}
	return pid, nil
}

func reviewerGroupGone(pid int) (bool, error) {
	err := syscall.Kill(-pid, 0)
	if errors.Is(err, syscall.ESRCH) {
		return true, nil
	}
	return false, err
}

func missingTmuxServer(result agentruntime.Result) bool {
	message := strings.TrimSpace(result.Output)
	return result.Exited && result.Code == 1 && (strings.HasPrefix(message, "error connecting to ") && strings.HasSuffix(message, " (No such file or directory)") || strings.HasPrefix(message, "no server running on /"))
}

func verifyReviewerChildBinding(ctx context.Context, boundary boundaryCaller, env []string, session, launchPath, terminalPath string, identity reviewerLaunchIdentity, candidate int) error {
	if candidate < 2 {
		return errors.New("reviewer child process identity is missing")
	}
	pane := agentruntime.PaneTarget(session)
	started, err := boundary.call(ctx, "run", agentruntime.Command{Name: "tmux", Args: []string{"display-message", "-p", "-t", pane, "#{pane_start_command}"}, Env: env})
	matching := reviewerPaneStartMatches(started.Output, launchPath, terminalPath, identity)
	if launchPath == "" && terminalPath == "" {
		matching = strings.Contains(started.Output, " review-pane tmux ") && strings.Contains(started.Output, reviewerSignal(identity))
	}
	if err != nil || started.Exited || !matching {
		return errors.New("exact reviewer wrapper is not live")
	}
	pidResult, err := boundary.call(ctx, "run", agentruntime.Command{Name: "tmux", Args: []string{"display-message", "-p", "-t", pane, "#{pane_pid}"}, Env: env})
	if err != nil || pidResult.Exited {
		return errors.New("exact reviewer wrapper PID is unavailable")
	}
	wrapperPID, err := reviewerPanePID(pidResult.Output)
	if err != nil {
		return err
	}
	parentResult, err := exec.CommandContext(ctx, "ps", "-o", "ppid=", "-p", strconv.Itoa(candidate)).Output()
	if err != nil {
		return fmt.Errorf("verify reviewer child parent: %w", err)
	}
	parentPID, err := reviewerPanePID(string(parentResult))
	if err != nil || parentPID != wrapperPID {
		return errors.New("reviewer child is not owned by the exact wrapper")
	}
	groupPID, err := syscall.Getpgid(candidate)
	if err != nil || groupPID != candidate {
		return errors.New("reviewer child is not its process-group leader")
	}
	return nil
}

func readReviewerRecord(path string, value any) (bool, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() < 1 || info.Size() > 4096 {
		return false, errors.New("reviewer lifecycle record is unsafe")
	}
	file, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return false, errors.New("reviewer lifecycle record changed while opening")
	}
	body, err := io.ReadAll(io.LimitReader(file, 4097))
	if err != nil || len(body) != int(info.Size()) {
		return false, errors.New("reviewer lifecycle record changed while reading")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if decoder.Decode(value) != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return false, errors.New("reviewer lifecycle record is invalid")
	}
	return true, nil
}

func writeReviewerRecord(path string, value any) error {
	body, err := json.Marshal(value)
	if err != nil || len(body) > 4096 {
		return errors.New("reviewer lifecycle record is invalid")
	}
	root := filepath.Dir(path)
	file, err := os.CreateTemp(root, ".reviewer-record-")
	if err != nil {
		return err
	}
	temporary := file.Name()
	defer os.Remove(temporary)
	if err := file.Chmod(0o640); err != nil {
		file.Close()
		return err
	}
	if _, err := file.Write(body); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Link(temporary, path); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return err
		}
		var existing json.RawMessage
		if ok, readErr := readReviewerRecord(path, &existing); !ok || readErr != nil || !bytes.Equal(existing, body) {
			return errors.New("reviewer lifecycle record collision")
		}
	}
	return immutableDirSync(root)
}

// runReviewerPane is an internal tmux command, not an operator action.
func runReviewerPane(args []string, stdout, stderr io.Writer) (int, syscall.Signal, error) {
	if len(args) < 8 || args[6] != "--" || args[0] == "" || !filepath.IsAbs(args[1]) || !filepath.IsAbs(args[2]) || args[7] == "" {
		return 125, 0, errors.New("invalid internal reviewer pane invocation")
	}
	var identity reviewerLaunchIdentity
	if json.Unmarshal([]byte(args[5]), &identity) != nil || identity.EffectID == "" || identity.IssueGeneration == 0 || identity.AttemptGeneration == 0 || identity.RequestDigest == "" || reviewerSignal(identity) != args[3] || reviewerStartSignal(identity) != args[4] {
		return 125, 0, errors.New("invalid reviewer launch identity")
	}
	launchPath, terminalPath := args[1], args[2]
	if filepath.Dir(launchPath) != filepath.Dir(terminalPath) || filepath.Base(launchPath) != "launch.json" || filepath.Base(terminalPath) != "terminal.json" {
		return 125, 0, errors.New("reviewer lifecycle paths do not match")
	}
	defer func() {
		unlock := exec.Command("tmux", "wait-for", "-U", args[3])
		unlock.Dir = "/tmp"
		_ = unlock.Run()
		unlockStart := exec.Command("tmux", "wait-for", "-U", args[4])
		unlockStart.Dir = "/tmp"
		_ = unlockStart.Run()
	}()
	lock := exec.Command(args[0], "wait-for", "-L", reviewerGoSignal(identity))
	lock.Dir = "/tmp"
	if err := lock.Run(); err != nil {
		return 125, 0, fmt.Errorf("lock reviewer start gate: %w", err)
	}
	reader, writer, err := os.Pipe()
	if err != nil {
		return 125, 0, err
	}
	defer reader.Close()
	defer writer.Close()
	gateCtx, cancelGate := context.WithCancel(context.Background())
	defer cancelGate()
	code, childSignal, err := agentruntime.RunPaneCommandAfterStart(context.Background(), args[0], args[7:], os.Stdin, stdout, stderr, func(pid int) error {
		launch := identity
		launch.ChildPID = pid
		if err := writeReviewerRecord(launchPath, launch); err != nil {
			return err
		}
		_ = reader.Close()
		unlock := exec.Command("tmux", "wait-for", "-U", args[4])
		unlock.Dir = "/tmp"
		if err := unlock.Run(); err != nil {
			return err
		}
		go func() {
			wait := exec.CommandContext(gateCtx, args[0], "wait-for", "-L", reviewerGoSignal(identity))
			wait.Dir = "/tmp"
			if wait.Run() == nil {
				_, _ = io.WriteString(writer, "go\n")
				unlock := exec.Command(args[0], "wait-for", "-U", reviewerGoSignal(identity))
				unlock.Dir = "/tmp"
				_ = unlock.Run()
			}
			_ = writer.Close()
		}()
		return nil
	}, reader)
	record := reviewerTerminalRecord{Identity: identity, ExitCode: code, Signal: int(childSignal)}
	if writeErr := writeReviewerRecord(terminalPath, record); writeErr != nil {
		return 126, 0, errors.Join(err, fmt.Errorf("record reviewer terminal result: %w", writeErr))
	}
	return code, childSignal, err
}

func readReviewerTerminal(launchPath, terminalPath string, identity reviewerLaunchIdentity) (*reviewerTerminalRecord, error) {
	var launch reviewerLaunchIdentity
	if exists, err := readReviewerRecord(launchPath, &launch); err != nil || !exists || !sameReviewerIdentity(launch, identity) {
		if err != nil {
			return nil, err
		}
		return nil, errors.New("reviewer launch identity is missing or mismatched")
	}
	var terminal reviewerTerminalRecord
	exists, err := readReviewerRecord(terminalPath, &terminal)
	if err != nil || !exists {
		return nil, err
	}
	if !sameReviewerIdentity(terminal.Identity, identity) || terminal.ExitCode < 0 || terminal.ExitCode > 255 || terminal.Signal < 0 || terminal.Signal > 127 || terminal.Signal != 0 && terminal.ExitCode != 128+terminal.Signal {
		return nil, errors.New("reviewer terminal identity or exit result is invalid")
	}
	return &terminal, nil
}

func reviewerTerminalDiagnostic(record reviewerTerminalRecord) string {
	if record.Signal != 0 {
		return "reviewer terminated by signal " + strconv.Itoa(record.Signal)
	}
	return "reviewer exited " + strconv.Itoa(record.ExitCode)
}

func reviewerLifecycleError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %s", errReviewerTerminal, strings.ToValidUTF8(err.Error(), "?"))
}
