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
	for {
		pids, err := activeLinuxProcessGroupMembers(pgid)
		if err != nil || len(pids) == 0 {
			return len(pids) == 0, err
		}
		fds := make([]int, 0, len(pids))
		for _, pid := range pids {
			fd, _, errno := syscall.Syscall(linuxPIDFDOpen, uintptr(pid), 0, 0)
			if errno == 0 {
				fds = append(fds, int(fd))
			} else if errno != syscall.ESRCH {
				for _, open := range fds {
					_ = syscall.Close(open)
				}
				return false, errno
			}
		}
		if len(fds) == 0 {
			continue
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			for _, fd := range fds {
				_ = syscall.Close(fd)
			}
			return false, nil
		}
		var ready syscall.FdSet
		maxFD := 0
		for _, fd := range fds {
			if fd >= len(ready.Bits)*64 {
				for _, open := range fds {
					_ = syscall.Close(open)
				}
				return false, errors.New("implementation pidfd exceeds select capacity")
			}
			ready.Bits[fd/64] |= 1 << uint(fd%64)
			if fd > maxFD {
				maxFD = fd
			}
		}
		timeout := syscall.NsecToTimeval(remaining.Nanoseconds())
		_, selectErr := syscall.Select(maxFD+1, &ready, nil, nil, &timeout)
		for _, fd := range fds {
			_ = syscall.Close(fd)
		}
		if selectErr != nil && selectErr != syscall.EINTR {
			return false, selectErr
		}
	}
}

func activeLinuxProcessGroupMembers(pgid int) ([]int, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, err
	}
	var pids []int
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid < 2 {
			continue
		}
		body, err := os.ReadFile(filepath.Join("/proc", entry.Name(), "stat"))
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
