//go:build !linux

package runtime

import (
	"errors"
	"syscall"
)

func implementationGroupTerminated(pgid int) (bool, error) {
	err := syscall.Kill(-pgid, 0)
	if errors.Is(err, syscall.ESRCH) {
		return true, nil
	}
	return false, err
}
