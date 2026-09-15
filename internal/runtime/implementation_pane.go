package runtime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"syscall"
	"time"
)

// The keeper pins the child's process-group identity until its original
// parent has killed every group member. The child exec gate preserves its PID
// and WaitStatus while durable group evidence is written.
const boundInteractiveGate = `IFS= read -r ready <&3 || exit 125
[ "$ready" = go ] || exit 125
exec "$@" 3>&-`

// RunBoundPaneCommand is the v2 interactive pane helper. No worker command
// runs before its exact group start record is fsynced.
func RunBoundPaneCommand(ctx context.Context, manifest Manifest, tmux string, command []string, stdin io.Reader, stdout, stderr io.Writer) (int, syscall.Signal, error) {
	if len(command) == 0 || command[0] == "" {
		return 125, 0, errors.New("bound pane command is missing")
	}
	binding, err := ReadImplementationBinding(manifest)
	if err != nil || os.Getenv("TMUX_PANE") != binding.PaneID || os.Getpid() != binding.PanePID {
		return 125, 0, errors.Join(err, errors.New("bound pane identity is unavailable"))
	}
	holdReader, holdWriter, err := os.Pipe()
	if err != nil {
		return 125, 0, err
	}
	defer holdReader.Close()
	defer holdWriter.Close()
	keeper := exec.Command("/bin/sh", "-c", `IFS= read -r _ <&3`)
	keeper.ExtraFiles = []*os.File{holdReader}
	keeper.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := keeper.Start(); err != nil {
		return 125, 0, err
	}
	_ = holdReader.Close()
	gateReader, gateWriter, err := os.Pipe()
	if err != nil {
		_ = syscall.Kill(-keeper.Process.Pid, syscall.SIGKILL)
		_ = keeper.Wait()
		return 125, 0, err
	}
	defer gateReader.Close()
	defer gateWriter.Close()
	worker := exec.Command("/bin/sh", append([]string{"-c", boundInteractiveGate, "bound-interactive"}, command...)...)
	worker.Stdin, worker.Stdout, worker.Stderr = stdin, stdout, stderr
	worker.ExtraFiles = []*os.File{gateReader}
	worker.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pgid: keeper.Process.Pid}
	if terminal, ok := stdin.(*os.File); ok {
		if info, statErr := terminal.Stat(); statErr == nil && info.Mode()&os.ModeCharDevice != 0 {
			// Interactive workers have their own killable group. Make that
			// group foreground before it reads the tmux PTY, or reads fail
			// with EIO even though the pane is visibly live.
			worker.SysProcAttr.Foreground = true
			worker.SysProcAttr.Ctty = 0
		}
	}
	if err := worker.Start(); err != nil {
		_ = syscall.Kill(-keeper.Process.Pid, syscall.SIGKILL)
		_ = keeper.Wait()
		return 125, 0, err
	}
	_ = gateReader.Close()
	groupKilled := false
	var groupKillErr error
	stopGroup := func() error {
		if !groupKilled {
			groupKillErr = syscall.Kill(-keeper.Process.Pid, syscall.SIGKILL)
			groupKilled = true
		}
		killErr := groupKillErr
		if errors.Is(killErr, syscall.ESRCH) {
			killErr = nil
		}
		_ = holdWriter.Close()
		_ = keeper.Wait()
		if killErr != nil {
			return fmt.Errorf("kill bound interactive group %d: %w", keeper.Process.Pid, killErr)
		}
		return nil
	}
	start, err := WriteImplementationGroupStart(manifest, binding, "interactive", keeper.Process.Pid, worker.Process.Pid)
	if err != nil {
		_ = stopGroup()
		_ = worker.Wait()
		return 125, 0, err
	}
	if _, err := io.WriteString(gateWriter, "go\n"); err != nil {
		_ = stopGroup()
		_ = worker.Wait()
		return 125, 0, errors.Join(err, WriteImplementationGroupDead(manifest, binding, start))
	}
	_ = gateWriter.Close()
	waited := make(chan error, 1)
	go func() { waited <- worker.Wait() }()
	cancelled := false
	var waitErr error
	select {
	case waitErr = <-waited:
	case <-ctx.Done():
		cancelled = true
		groupKillErr = syscall.Kill(-keeper.Process.Pid, syscall.SIGKILL)
		groupKilled = true
		waitErr = <-waited
	}
	cleanupErr := errors.Join(stopGroup(), WriteImplementationGroupDead(manifest, binding, start))
	if cleanupErr != nil {
		return 125, 0, cleanupErr
	}
	if cancelled {
		return 137, syscall.SIGKILL, nil
	}
	status, ok := worker.ProcessState.Sys().(syscall.WaitStatus)
	if !ok {
		return 125, 0, errors.New("bound pane child wait status is unavailable")
	}
	statusCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if status.Signaled() {
		signal := status.Signal()
		return 128 + int(signal), signal, recordPaneExitOption(statusCtx, tmux, PaneExitSignalOption, int(signal))
	}
	code := status.ExitStatus()
	if code < 0 || code > 255 {
		return 125, 0, fmt.Errorf("invalid bound pane exit status %d", code)
	}
	if waitErr != nil {
		var exit *exec.ExitError
		if !errors.As(waitErr, &exit) {
			return code, 0, waitErr
		}
	}
	return code, 0, RecordPaneExitStatus(statusCtx, tmux, code)
}
