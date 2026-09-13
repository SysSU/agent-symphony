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
}

type reviewerTerminalRecord struct {
	Identity reviewerLaunchIdentity `json:"identity"`
	ExitCode int                    `json:"exit_code"`
	Signal   int                    `json:"signal,omitempty"`
}

var errReviewerTerminal = errors.New("reviewer terminal failure")

func reviewerIdentity(identity stateResultIdentity) reviewerLaunchIdentity {
	return reviewerLaunchIdentity{identity.EffectID, identity.IssueGeneration, identity.AttemptGeneration, identity.RequestDigest}
}

func reviewerLifecyclePaths(snapshot, target string) (string, string) {
	root := filepath.Dir(reviewResultPath(snapshot, target))
	return filepath.Join(root, "launch.json"), filepath.Join(root, "terminal.json")
}

func reviewerSignal(identity reviewerLaunchIdentity) string { return "review-" + identity.EffectID }
func reviewerStartSignal(identity reviewerLaunchIdentity) string {
	return reviewerSignal(identity) + "-start"
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
	code, childSignal, err := agentruntime.RunPaneCommandAfterStart(context.Background(), args[0], args[7:], os.Stdin, stdout, stderr, func() error {
		if err := writeReviewerRecord(launchPath, identity); err != nil {
			return err
		}
		unlock := exec.Command("tmux", "wait-for", "-U", args[4])
		unlock.Dir = "/tmp"
		return unlock.Run()
	})
	record := reviewerTerminalRecord{Identity: identity, ExitCode: code, Signal: int(childSignal)}
	if writeErr := writeReviewerRecord(terminalPath, record); writeErr != nil {
		return 126, 0, errors.Join(err, fmt.Errorf("record reviewer terminal result: %w", writeErr))
	}
	return code, childSignal, err
}

func readReviewerTerminal(launchPath, terminalPath string, identity reviewerLaunchIdentity) (*reviewerTerminalRecord, error) {
	var launch reviewerLaunchIdentity
	if exists, err := readReviewerRecord(launchPath, &launch); err != nil || !exists || !reflect.DeepEqual(launch, identity) {
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
	if !reflect.DeepEqual(terminal.Identity, identity) || terminal.ExitCode < 0 || terminal.ExitCode > 255 || terminal.Signal < 0 || terminal.Signal > 127 || terminal.Signal != 0 && terminal.ExitCode != 128+terminal.Signal {
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
