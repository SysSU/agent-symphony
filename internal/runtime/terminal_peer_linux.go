//go:build linux

package runtime

import (
	"errors"
	"net"
	"os"
	"syscall"
	"time"
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
	// A readable pidfd is positive kernel evidence of exit, even while the
	// tmux parent has not reaped the zombie. A live replacement stays unproved.
	n, err := pollLinuxPIDFDs([]int{fd}, 0)
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
	for {
		_, err := pollLinuxPIDFDs([]int{fd}, -1)
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		return err
	}
}

func pollLinuxPIDFDs(fds []int, timeout time.Duration) (int, error) {
	epollFD, err := syscall.EpollCreate1(syscall.EPOLL_CLOEXEC)
	if err != nil {
		return 0, err
	}
	defer syscall.Close(epollFD)
	for _, fd := range fds {
		event := syscall.EpollEvent{Events: syscall.EPOLLIN, Fd: int32(fd)}
		if err := syscall.EpollCtl(epollFD, syscall.EPOLL_CTL_ADD, fd, &event); err != nil {
			return 0, err
		}
	}
	waitMilliseconds := -1
	if timeout >= 0 {
		waitMilliseconds = int((timeout + time.Millisecond - 1) / time.Millisecond)
	}
	return syscall.EpollWait(epollFD, make([]syscall.EpollEvent, len(fds)), waitMilliseconds)
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
