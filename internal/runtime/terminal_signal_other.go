//go:build !darwin && !linux

package runtime

import (
	"os"
	"os/exec"
)

func execCommand(name string, args ...string) *exec.Cmd { return exec.Command(name, args...) }

func terminalNotifyResize(chan<- os.Signal) {}
func terminalStopResize(chan<- os.Signal)   {}
