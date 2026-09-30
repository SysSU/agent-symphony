//go:build linux

package runtime

import (
	"errors"
	"net"
	"os"
	"syscall"
)

func terminalSocketIdentity(info os.FileInfo) (uint64, uint64, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Dev == 0 || stat.Ino == 0 {
		return 0, 0, errors.New("terminal socket identity is unavailable")
	}
	return uint64(stat.Dev), uint64(stat.Ino), nil
}

func verifyTerminalPeerPID(conn *net.UnixConn, expected int) error {
	raw, err := conn.SyscallConn()
	if err != nil {
		return err
	}
	pid := 0
	var controlErr error
	if err := raw.Control(func(fd uintptr) {
		var credentials *syscall.Ucred
		credentials, controlErr = syscall.GetsockoptUcred(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
		if credentials != nil {
			pid = int(credentials.Pid)
		}
	}); err != nil {
		return err
	}
	if controlErr != nil || pid != expected {
		return errors.New("terminal broker peer PID mismatch")
	}
	return nil
}

func terminalMakeRaw(file *os.File) error {
	command := execCommand("stty", "raw", "-echo")
	command.Stdin = file
	return command.Run()
}

func terminalRestore(file *os.File) {
	command := execCommand("stty", "sane")
	command.Stdin = file
	_ = command.Run()
}
