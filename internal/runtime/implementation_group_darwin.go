//go:build darwin

package runtime

import (
	"errors"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	darwinImplementationGroupExitWait = 30 * time.Second
	darwinImplementationGroupPoll     = 250 * time.Millisecond
)

// Darwin keeps killed orphaned descendants as zombies until launchd reaps
// them. Zombies have no execution authority, so enumerate the exact process
// group instead of treating kill(0)'s zombie visibility as liveness.
func implementationGroupTerminated(pgid int) (bool, error) {
	return waitForDarwinProcessGroup(pgid, func(pgid int) error { return syscall.Kill(-pgid, 0) }, activeDarwinProcessGroupMembers)
}

func waitForDarwinProcessGroup(pgid int, probe func(int) error, activeMembers func(int) (bool, error)) (bool, error) {
	if err := probe(pgid); errors.Is(err, syscall.ESRCH) {
		return true, nil
	} else if err != nil && !errors.Is(err, syscall.EPERM) {
		return false, err
	}
	// Darwin may return EPERM for a killed, zombie-only group. Inventory is
	// still authoritative: any active member keeps termination unproved.
	deadline := time.Now().Add(darwinImplementationGroupExitWait)
	for {
		active, err := activeMembers(pgid)
		if err != nil || !active {
			return !active, err
		}
		if time.Now().After(deadline) {
			return false, nil
		}
		time.Sleep(darwinImplementationGroupPoll)
	}
}

func activeDarwinProcessGroupMembers(pgid int) (bool, error) {
	body, err := exec.Command("/bin/ps", "-axo", "pid=,pgid=,state=").Output()
	if err != nil {
		return false, err
	}
	return parseDarwinProcessGroup(body, pgid)
}

func parseDarwinProcessGroup(body []byte, pgid int) (bool, error) {
	for _, line := range strings.Split(string(body), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		if len(fields) != 3 {
			return false, errors.New("invalid Darwin process inventory")
		}
		pid, pidErr := strconv.Atoi(fields[0])
		group, groupErr := strconv.Atoi(fields[1])
		if pidErr != nil || groupErr != nil || pid < 1 || group < 1 || fields[2] == "" {
			return false, errors.New("invalid Darwin process inventory")
		}
		if group == pgid && fields[2][0] != 'Z' {
			return true, nil
		}
	}
	return false, nil
}
