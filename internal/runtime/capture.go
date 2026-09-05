package runtime

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	internalgithub "github.com/SysSU/agent-symphony/internal/github"
)

var (
	// ErrWorkerResultOverflow means the configured command exceeded the capture ceiling.
	ErrWorkerResultOverflow = errors.New("worker stdout exceeds 64 KiB")
	// ErrWorkerOutputOpen means an out-of-group process retained the worker output descriptor.
	ErrWorkerOutputOpen = errors.New("worker stdout remained open after process-group termination")
)

const (
	workerCaptureDrainTimeout = 500 * time.Millisecond
	workerReadyTimeout        = 15 * time.Second
)

type readyWriter struct {
	io.Writer
	ready func()
}

func (w readyWriter) Write(p []byte) (int, error) {
	n, err := w.Writer.Write(p)
	if n > 0 {
		w.ready()
	}
	return n, err
}

// The wrapper pins the process-group identity until its parent signals the group.
const workerWrapper = `set +m
"$@" 3>&- 4>&-
code=$?
printf '%d\n' "$code" >&3
IFS= read -r _ <&4`

// CaptureWorker runs command with a tmux prompt and bounded result channel.
func CaptureWorker(ctx context.Context, tmux, buffer, resultPath string, command []string, stdout, stderr io.Writer) (int, error) {
	return captureWorker(ctx, tmux, buffer, resultPath, command, stdout, stderr, "/tmp", false)
}

// CaptureWorkerReplacingResult atomically replaces an earlier result only
// after the follow-up worker has completed and its output is durable.
func CaptureWorkerReplacingResult(ctx context.Context, tmux, buffer, resultPath string, command []string, stdout, stderr io.Writer) (int, error) {
	return captureWorkerAfterStart(ctx, tmux, buffer, resultPath, command, stdout, stderr, "/tmp", true, nil)
}

func captureWorker(ctx context.Context, tmux, buffer, resultPath string, command []string, stdout, stderr io.Writer, tempDir string, replace bool) (int, error) {
	return captureWorkerAfterStart(ctx, tmux, buffer, resultPath, command, stdout, stderr, tempDir, replace, nil)
}

// CaptureWorkerReplacingResultAfterStart acknowledges a handoff only after its
// replacement worker has started successfully.
func CaptureWorkerReplacingResultAfterStart(ctx context.Context, tmux, buffer, resultPath string, command []string, stdout, stderr io.Writer, afterStart func() error) (int, error) {
	return captureWorkerAfterStart(ctx, tmux, buffer, resultPath, command, stdout, stderr, "/tmp", true, afterStart)
}

func captureWorkerAfterStart(ctx context.Context, tmux, buffer, resultPath string, command []string, stdout, stderr io.Writer, tempDir string, replace bool, afterStart func() error) (int, error) {
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
	save.Dir = tempDir
	save.Stdout, save.Stderr = prompt, stderr
	if err := save.Run(); err != nil {
		return 1, fmt.Errorf("save prompt buffer: %w", err)
	}
	deleteBuffer := exec.CommandContext(ctx, tmux, "delete-buffer", "-b", buffer)
	deleteBuffer.Dir = tempDir
	deleteBuffer.Stderr = stderr
	if err := deleteBuffer.Run(); err != nil {
		return 1, fmt.Errorf("delete prompt buffer: %w", err)
	}
	if _, err := prompt.Seek(0, io.SeekStart); err != nil {
		return 1, err
	}
	if _, err := exec.LookPath(command[0]); err != nil {
		return 1, err
	}
	statusReader, statusWriter, err := os.Pipe()
	if err != nil {
		return 1, err
	}
	defer statusReader.Close()
	defer statusWriter.Close()
	holdReader, holdWriter, err := os.Pipe()
	if err != nil {
		return 1, err
	}
	defer holdReader.Close()
	defer holdWriter.Close()

	args := append([]string{"-c", workerWrapper, "agent-symphony-worker"}, command...)
	child := exec.Command("/bin/sh", args...)
	ready := make(chan struct{})
	var readyOnce sync.Once
	markReady := func() { readyOnce.Do(func() { close(ready) }) }
	child.Stdin, child.Stderr, child.Env = prompt, readyWriter{Writer: stderr, ready: markReady}, captureEnvironment(os.Environ())
	child.ExtraFiles = []*os.File{statusWriter, holdReader}
	child.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	var result *os.File
	resultName := resultPath
	if resultPath != "" {
		if replace {
			result, err = os.CreateTemp(filepath.Dir(resultPath), "."+filepath.Base(resultPath)+"-")
			if err == nil {
				resultName = result.Name()
				err = result.Chmod(0o600)
			}
		} else {
			result, err = os.OpenFile(resultPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		}
		if err != nil {
			return 1, fmt.Errorf("create worker result: %w", err)
		}
		defer func() {
			_ = result.Close()
			if replace {
				_ = os.Remove(resultName)
			}
		}()
	}
	workerOutput, err := child.StdoutPipe()
	if err != nil {
		return 1, err
	}
	pipe, ok := workerOutput.(*os.File)
	if !ok {
		return 1, errors.New("worker stdout pipe is not an OS file")
	}
	if err := child.Start(); err != nil {
		return 1, err
	}
	pipeFD := int(pipe.Fd())
	if err := syscall.SetNonblock(pipeFD, true); err != nil {
		_ = killProcessGroup(child)
		_ = child.Wait()
		return 1, err
	}
	statusWriter.Close()
	holdReader.Close()
	type completion struct {
		code int
		err  error
	}
	completed := make(chan completion, 1)
	go func(destination chan<- completion) {
		code, err := readWorkerStatus(statusReader)
		destination <- completion{code: code, err: err}
	}(completed)
	stopped := make(chan struct{})
	captured := make(chan error, 1)
	destination, bounded := stdout, false
	var output bytes.Buffer
	if resultPath != "" {
		destination, bounded = &output, true
	}
	destination = readyWriter{Writer: destination, ready: markReady}
	go func() { captured <- captureWorkerOutput(destination, pipeFD, stopped, bounded) }()
	if afterStart != nil {
		timer := time.NewTimer(workerReadyTimeout)
		select {
		case <-ready:
			timer.Stop()
			if err := afterStart(); err != nil {
				_ = killProcessGroup(child)
				close(stopped)
				<-captured
				_ = pipe.Close()
				_ = child.Wait()
				return 1, err
			}
		case <-timer.C:
			_ = killProcessGroup(child)
			close(stopped)
			<-captured
			_ = pipe.Close()
			_ = child.Wait()
			return 1, errors.New("worker produced no startup output within 15 seconds")
		case <-ctx.Done():
			timer.Stop()
			_ = killProcessGroup(child)
			close(stopped)
			<-captured
			_ = pipe.Close()
			_ = child.Wait()
			return 1, ctx.Err()
		}
	}
	if !bounded {
		var finished completion
		select {
		case finished = <-completed:
		case <-ctx.Done():
		}
		cleanupErr := killProcessGroup(child)
		close(stopped)
		captureErr := <-captured
		_ = pipe.Close()
		_ = child.Wait()
		if ctx.Err() != nil {
			return 1, ctx.Err()
		}
		if cleanupErr != nil {
			return 1, cleanupErr
		}
		if captureErr != nil {
			return 1, captureErr
		}
		if finished.err != nil {
			return 1, fmt.Errorf("read worker status: %w", finished.err)
		}
		return finished.code, nil
	}

	var finished completion
	var captureErr error
	for completed != nil && captureErr == nil {
		select {
		case finished = <-completed:
			completed = nil
		case captureErr = <-captured:
			captured = nil
		case <-ctx.Done():
			completed = nil
		}
	}
	cleanupErr := killProcessGroup(child)
	close(stopped)
	if captured != nil {
		captureErr = <-captured
	}
	_ = pipe.Close()
	_ = child.Wait()
	if bounded {
		redacted := internalgithub.RedactEnvironment(output.String(), child.Env)
		if len(redacted) > WorkerResultMaxBytes {
			return 1, ErrWorkerResultOverflow
		}
		if count, err := io.WriteString(result, redacted); err != nil {
			return 1, fmt.Errorf("write worker result: %w", err)
		} else if count != len(redacted) {
			return 1, fmt.Errorf("write worker result: %w", io.ErrShortWrite)
		}
	}
	if ctx.Err() != nil {
		return 1, ctx.Err()
	}
	if cleanupErr != nil {
		return 1, cleanupErr
	}
	if captureErr != nil {
		return 1, captureErr
	}
	if finished.err != nil {
		return 1, fmt.Errorf("read worker status: %w", finished.err)
	}
	if err := result.Sync(); err != nil {
		return 1, err
	}
	if replace {
		if err := result.Close(); err != nil {
			return 1, err
		}
		if err := os.Rename(resultName, resultPath); err != nil {
			return 1, err
		}
		dir, err := os.Open(filepath.Dir(resultPath))
		if err != nil {
			return 1, err
		}
		err = dir.Sync()
		closeErr := dir.Close()
		if err != nil {
			return 1, err
		}
		if closeErr != nil {
			return 1, closeErr
		}
	}
	return finished.code, nil
}

func killProcessGroup(command *exec.Cmd) error {
	if command.Process != nil {
		if err := syscall.Kill(-command.Process.Pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
			return err
		}
	}
	return nil
}

func readWorkerStatus(reader io.Reader) (int, error) {
	line, err := bufio.NewReader(reader).ReadString('\n')
	if err != nil {
		return 0, err
	}
	code, err := strconv.Atoi(strings.TrimSpace(line))
	if err != nil || code < 0 || code > 255 {
		return 0, errors.New("invalid worker exit status")
	}
	return code, nil
}

func captureWorkerOutput(result io.Writer, pipeFD int, stopped <-chan struct{}, bounded bool) error {
	var buffer [32 << 10]byte
	written := 0
	var stopDeadline time.Time
	for {
		if stopDeadline.IsZero() {
			select {
			case <-stopped:
				stopDeadline = time.Now().Add(workerCaptureDrainTimeout)
			default:
			}
		} else if time.Now().After(stopDeadline) {
			return ErrWorkerOutputOpen
		}
		n, err := syscall.Read(pipeFD, buffer[:])
		if n > 0 {
			if bounded && written+n > WorkerResultMaxBytes {
				n = WorkerResultMaxBytes - written
				if n > 0 {
					if count, writeErr := result.Write(buffer[:n]); writeErr != nil {
						return fmt.Errorf("capture worker stdout: %w", writeErr)
					} else if count != n {
						return fmt.Errorf("capture worker stdout: %w", io.ErrShortWrite)
					}
				}
				return ErrWorkerResultOverflow
			}
			written += n
			if count, writeErr := result.Write(buffer[:n]); writeErr != nil {
				return fmt.Errorf("capture worker stdout: %w", writeErr)
			} else if count != n {
				return fmt.Errorf("capture worker stdout: %w", io.ErrShortWrite)
			}
		}
		if n == 0 && err == nil {
			return nil
		}
		if err == nil || errors.Is(err, syscall.EINTR) {
			continue
		}
		if !errors.Is(err, syscall.EAGAIN) && !errors.Is(err, syscall.EWOULDBLOCK) {
			return fmt.Errorf("capture worker stdout: %w", err)
		}
		if stopDeadline.IsZero() {
			select {
			case <-stopped:
				stopDeadline = time.Now().Add(workerCaptureDrainTimeout)
			case <-time.After(5 * time.Millisecond):
			}
		} else {
			time.Sleep(5 * time.Millisecond)
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
