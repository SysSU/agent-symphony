//go:build !linux && !darwin

package runtime

import (
	"errors"
	"syscall"
	"time"
)

func implementationGroupTerminated(pgid int) (bool, error) {
	deadline := time.Now().Add(2 * time.Second)
	for {
		err := syscall.Kill(-pgid, 0)
		if errors.Is(err, syscall.ESRCH) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		if time.Now().After(deadline) {
			return false, nil
		}
		time.Sleep(10 * time.Millisecond)
	}
}
