package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"
)

func TerminalBrokerReadyChannel(launchID string) string { return "terminal-broker-" + launchID }

func TerminalBrokerSocketDir(manifest Manifest) string {
	return TerminalBrokerSocketDirForState(filepath.Dir(filepath.Dir(filepath.Dir(filepath.Dir(manifest.LogPath)))))
}

func TerminalBrokerSocketDirForState(stateRoot string) string {
	digest := sha256.Sum256([]byte(filepath.Clean(stateRoot)))
	return filepath.Join("/tmp", "as-t-"+hex.EncodeToString(digest[:12]))
}

func prepareTerminalBrokerDir(stateRoot string) (string, error) {
	directory := TerminalBrokerSocketDirForState(stateRoot)
	if err := os.Mkdir(directory, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return "", err
	}
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o700 {
		return "", errors.New("terminal broker socket directory is unsafe")
	}
	return directory, nil
}

func PrepareTerminalBrokerDir(stateRoot string) (string, error) {
	return prepareTerminalBrokerDir(stateRoot)
}

// RunBoundTerminalBroker is the exact tmux pane process. The configured
// command runs only as its separately-sessioned, owner-released child.
func RunBoundTerminalBroker(ctx context.Context, manifest Manifest, tmux, role string, command []string, stdin *os.File, stdout, stderr io.Writer) (int, syscall.Signal, error) {
	binding, err := ReadImplementationBinding(manifest)
	if err != nil || binding.PaneID != os.Getenv("TMUX_PANE") || binding.PanePID != os.Getpid() || binding.Role != role {
		return 125, 0, errors.Join(err, errors.New("bound terminal broker pane identity is unavailable"))
	}
	socketDir := TerminalBrokerSocketDir(manifest)
	var group ImplementationGroupStart
	code, signal, runErr := RunTerminalBroker(ctx, TerminalBrokerPath(manifest), socketDir, command, stdin, stdout, stderr, func(terminal TerminalBrokerBinding) error {
		if terminal.OuterPID != binding.PanePID {
			return errors.New("terminal broker outer identity changed")
		}
		var startErr error
		group, startErr = WriteImplementationGroupStart(manifest, binding, role, terminal.InnerPGID, terminal.InnerPID)
		if startErr != nil {
			return startErr
		}
		unlock := exec.CommandContext(ctx, tmux, "wait-for", "-U", TerminalBrokerReadyChannel(manifest.LaunchID))
		unlock.Dir = "/tmp"
		return unlock.Run()
	})
	if group.GroupPID != 0 {
		runErr = errors.Join(runErr, WriteImplementationGroupDead(manifest, binding, group))
	}
	statusCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if signal != 0 {
		runErr = errors.Join(runErr, recordPaneExitOption(statusCtx, tmux, PaneExitSignalOption, int(signal)))
	} else {
		runErr = errors.Join(runErr, RecordPaneExitStatus(statusCtx, tmux, code))
	}
	return code, signal, runErr
}
