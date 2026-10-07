//go:build linux

package runtime

import (
	"errors"
	"net"
	"os"
	"syscall"
)

func terminalProcessExited(pid int) (bool, error) {
	fd, err := openLinuxPIDFD(pid)
	if errors.Is(err, syscall.ESRCH) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	defer syscall.Close(fd)
	var ready syscall.FdSet
	if fd >= len(ready.Bits)*64 {
		return false, errors.New("terminal pidfd exceeds select capacity")
	}
	ready.Bits[fd/64] |= 1 << uint(fd%64)
	timeout := syscall.Timeval{}
	// A readable pidfd is positive kernel evidence of exit, even while the
	// tmux parent has not reaped the zombie. A live replacement stays unproved.
	n, err := syscall.Select(fd+1, &ready, nil, nil, &timeout)
	if errors.Is(err, syscall.EINTR) {
		return false, nil
	}
	return err == nil && n == 1, err
}

func terminalWaitProcessExit(pid int) error {
	fd, err := openLinuxPIDFD(pid)
	if errors.Is(err, syscall.ESRCH) {
		return nil
	}
	if err != nil {
		return err
	}
	defer syscall.Close(fd)
	if fd >= len((syscall.FdSet{}).Bits)*64 {
		return errors.New("terminal pidfd exceeds select capacity")
	}
	for {
		var ready syscall.FdSet
		ready.Bits[fd/64] |= 1 << uint(fd%64)
		_, err := syscall.Select(fd+1, &ready, nil, nil, nil)
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		return err
	}
}

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
