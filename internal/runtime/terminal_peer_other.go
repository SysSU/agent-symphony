//go:build !darwin && !linux

package runtime

import (
	"errors"
	"net"
	"os"
)

func terminalProcessExited(int) (bool, error) {
	return false, errors.New("terminal process exit verification is unsupported")
}

func terminalSocketIdentity(os.FileInfo) (uint64, uint64, error) {
	return 0, 0, errors.New("terminal socket identity is unsupported")
}

func verifyTerminalPeerPID(*net.UnixConn, int) error {
	return errors.New("terminal broker peer PID verification is unsupported")
}

func terminalMakeRaw(*os.File) error { return errors.New("terminal raw mode is unsupported") }
func terminalRestore(*os.File)       {}
