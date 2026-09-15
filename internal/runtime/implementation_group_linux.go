//go:build linux

package runtime

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	implementationGroupExitWait = 2 * time.Second
	linuxPIDFDOpen              = 434
)

// implementationGroupTerminated waits on exact process handles. Linux keeps
// killed grandchildren as zombies until their new parent reaps them; zombies
// have no execution authority and do not make the group active.
func implementationGroupTerminated(pgid int) (bool, error) {
	if err := syscall.Kill(-pgid, 0); errors.Is(err, syscall.ESRCH) {
		return true, nil
	} else if err != nil {
		return false, err
	}
	deadline := time.Now().Add(implementationGroupExitWait)
	return waitForLinuxProcessGroup(pgid, func() time.Duration { return time.Until(deadline) }, activeLinuxProcessGroupMembers, openLinuxPIDFD, waitLinuxPIDFDs)
}

func waitForLinuxProcessGroup(pgid int, remaining func() time.Duration, scan func(int) ([]int, error), open func(int) (int, error), wait func([]int, time.Duration) error) (bool, error) {
	for {
		pids, err := scan(pgid)
		if err != nil {
			return false, err
		}
		if len(pids) == 0 {
			return true, nil
		}
		left := remaining()
		if left <= 0 {
			return false, nil
		}
		fds := make([]int, 0, len(pids))
		for _, pid := range pids {
			fd, err := open(pid)
			if err == nil {
				fds = append(fds, fd)
			} else if !errors.Is(err, syscall.ESRCH) {
				for _, open := range fds {
					_ = syscall.Close(open)
				}
				return false, err
			}
		}
		if len(fds) == 0 {
			continue
		}
		err = wait(fds, left)
		for _, fd := range fds {
			_ = syscall.Close(fd)
		}
		if err != nil && !errors.Is(err, syscall.EINTR) {
			return false, err
		}
	}
}

func openLinuxPIDFD(pid int) (int, error) {
	fd, _, errno := syscall.Syscall(linuxPIDFDOpen, uintptr(pid), 0, 0)
	if errno != 0 {
		return -1, errno
	}
	return int(fd), nil
}

func waitLinuxPIDFDs(fds []int, remaining time.Duration) error {
	var ready syscall.FdSet
	maxFD := 0
	for _, fd := range fds {
		if fd >= len(ready.Bits)*64 {
			return errors.New("implementation pidfd exceeds select capacity")
		}
		ready.Bits[fd/64] |= 1 << uint(fd%64)
		if fd > maxFD {
			maxFD = fd
		}
	}
	timeout := syscall.NsecToTimeval(remaining.Nanoseconds())
	_, err := syscall.Select(maxFD+1, &ready, nil, nil, &timeout)
	return err
}

func activeLinuxProcessGroupMembers(pgid int) ([]int, error) {
	return activeLinuxProcessGroupMembersAt("/proc", pgid)
}

func activeLinuxProcessGroupMembersAt(root string, pgid int) ([]int, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	var pids []int
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid < 2 {
			continue
		}
		body, err := os.ReadFile(filepath.Join(root, entry.Name(), "stat"))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		end := strings.LastIndexByte(string(body), ')')
		if end < 0 {
			return nil, errors.New("invalid Linux process stat")
		}
		fields := strings.Fields(string(body[end+1:]))
		if len(fields) < 3 {
			return nil, errors.New("invalid Linux process stat")
		}
		group, err := strconv.Atoi(fields[2])
		if err != nil {
			return nil, err
		}
		if group == pgid && fields[0] != "Z" && fields[0] != "X" {
			pids = append(pids, pid)
		}
	}
	return pids, nil
}
