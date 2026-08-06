package runtime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

// ErrWorkerResultOverflow means the configured command exceeded the capture ceiling.
var ErrWorkerResultOverflow = errors.New("worker stdout exceeds 64 KiB")

// CaptureWorker runs command with a tmux prompt and bounded result channel.
func CaptureWorker(ctx context.Context, tmux, buffer, resultPath string, command []string, stdout, stderr io.Writer) (int, error) {
	return captureWorker(ctx, tmux, buffer, resultPath, command, stdout, stderr, "/tmp")
}

func captureWorker(ctx context.Context, tmux, buffer, resultPath string, command []string, stdout, stderr io.Writer, tempDir string) (int, error) {
	if tmux == "" || buffer == "" || len(command) == 0 || command[0] == "" || (resultPath != "" && !filepath.IsAbs(resultPath)) {
		return 1, errors.New("invalid worker capture request")
	}
	prompt, err := os.CreateTemp(tempDir, "agent-symphony-prompt-")
	if err != nil {
		return 1, err
	}
	promptName := prompt.Name()
	if err := os.Remove(promptName); err != nil {
		prompt.Close()
		return 1, err
	}
	defer prompt.Close()
	save := exec.CommandContext(ctx, tmux, "save-buffer", "-b", buffer, "-")
	save.Stdout, save.Stderr = prompt, stderr
	if err := save.Run(); err != nil {
		return 1, fmt.Errorf("save prompt buffer: %w", err)
	}
	deleteBuffer := exec.CommandContext(ctx, tmux, "delete-buffer", "-b", buffer)
	deleteBuffer.Stderr = stderr
	if err := deleteBuffer.Run(); err != nil {
		return 1, fmt.Errorf("delete prompt buffer: %w", err)
	}
	if _, err := prompt.Seek(0, io.SeekStart); err != nil {
		return 1, err
	}

	child := exec.Command(command[0], command[1:]...)
	child.Stdin, child.Stderr, child.Env = prompt, stderr, captureEnvironment(os.Environ())
	child.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if resultPath == "" {
		child.Stdout = stdout
		if err := child.Start(); err != nil {
			return 1, err
		}
		stopCancel := context.AfterFunc(ctx, func() { killProcessGroup(child) })
		waitErr := child.Wait()
		stopCancel()
		cleanupErr := terminateProcessGroup(child)
		if ctx.Err() != nil {
			return 1, ctx.Err()
		}
		if cleanupErr != nil {
			return 1, cleanupErr
		}
		if waitErr != nil {
			return processExitCode(waitErr), nil
		}
		return 0, nil
	}

	result, err := os.OpenFile(resultPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return 1, fmt.Errorf("create worker result: %w", err)
	}
	defer result.Close()
	pipe, err := child.StdoutPipe()
	if err != nil {
		return 1, err
	}
	if err := child.Start(); err != nil {
		return 1, err
	}
	stopCancel := context.AfterFunc(ctx, func() { killProcessGroup(child) })
	killAndWait := func() {
		stopCancel()
		killProcessGroup(child)
		_ = child.Wait()
		_ = terminateProcessGroup(child)
	}
	if _, err := io.Copy(result, io.LimitReader(pipe, WorkerResultMaxBytes)); err != nil {
		killAndWait()
		return 1, fmt.Errorf("capture worker stdout: %w", err)
	}
	var extra [1]byte
	n, readErr := pipe.Read(extra[:])
	if n != 0 {
		killAndWait()
		return 1, ErrWorkerResultOverflow
	}
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		killAndWait()
		return 1, fmt.Errorf("capture worker stdout: %w", readErr)
	}
	waitErr := child.Wait()
	stopCancel()
	cleanupErr := terminateProcessGroup(child)
	if ctx.Err() != nil {
		return 1, ctx.Err()
	}
	if cleanupErr != nil {
		return 1, cleanupErr
	}
	if err := result.Sync(); err != nil {
		return 1, err
	}
	if waitErr != nil {
		return processExitCode(waitErr), nil
	}
	return 0, nil
}

func killProcessGroup(command *exec.Cmd) {
	if command.Process != nil {
		_ = syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
	}
}

func terminateProcessGroup(command *exec.Cmd) error {
	if command.Process == nil {
		return nil
	}
	group := -command.Process.Pid
	if err := syscall.Kill(group, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	for deadline := time.Now().Add(2 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		if err := syscall.Kill(group, 0); errors.Is(err, syscall.ESRCH) {
			return nil
		} else if err != nil {
			return err
		}
		if time.Now().After(deadline) {
			return errors.New("worker process group did not terminate")
		}
	}
}

func captureEnvironment(env []string) []string {
	filtered := make([]string, 0, len(env)+1)
	for _, value := range env {
		if !strings.HasPrefix(value, "TMPDIR=") {
			filtered = append(filtered, value)
		}
	}
	return append(filtered, "TMPDIR=/tmp")
}

func processExitCode(err error) int {
	var exit *exec.ExitError
	if errors.As(err, &exit) && exit.ExitCode() > 0 {
		return exit.ExitCode()
	}
	return 1
}
